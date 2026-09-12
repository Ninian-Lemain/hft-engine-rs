use crate::Engine;
use hft_events::{BoundedEventEngine, EventBatch, EventEngineConfigError};
use hft_gateway::Gateway;
use hft_journal::{JournalChannel, JournalReader};
use hft_recovery::{RecoveryError, recover_snapshot_and_tail};
use hft_risk::{RegistrationError, RiskEngine, RiskLimits};
use hft_spsc::{Consumer, QueueConfigError, SpscQueue};
use hft_types::{AccountId, InstrumentId};

#[derive(Debug)]
pub enum BuildError {
    ZeroCapacity,
    Registration(RegistrationError),
    Events(EventEngineConfigError),
    StorageAlreadyUsed,
    JournalStatusUnavailable,
    Recovery(RecoveryError),
    InstrumentMismatch,
}

/// Fixed event and journal storage for one engine lifetime.
/// A successful build consumes its initialization state. Recovery uses fresh storage.
pub struct EngineStorage<const BATCH: usize, const EVENTS: usize> {
    events: SpscQueue<EventBatch<BATCH>, EVENTS>,
    journal: JournalChannel,
    used: bool,
}

impl<const BATCH: usize, const EVENTS: usize> EngineStorage<BATCH, EVENTS> {
    /// # Errors
    ///
    /// Rejects an invalid event queue capacity.
    pub fn try_new() -> Result<Self, QueueConfigError> {
        Ok(Self {
            events: SpscQueue::try_new()?,
            journal: JournalChannel::try_new()?,
            used: false,
        })
    }
}

/// Queue consumers can move to separate event and persistence threads.
pub struct EngineParts<
    'storage,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const EVENTS: usize,
> {
    pub engine: Engine<'storage, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS, BATCH, EVENTS>,
    pub events: Consumer<'storage, EventBatch<BATCH>, EVENTS>,
    pub journal: JournalReader<'storage>,
}

pub struct EngineBuilder<
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
> {
    gateway: Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS>,
}

impl<
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
> EngineBuilder<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS>
{
    /// # Errors
    ///
    /// Rejects zero engine capacities, duplicate accounts, invalid risk limits,
    /// and account capacity exhaustion before any queue is attached.
    pub fn new(
        instrument: InstrumentId,
        accounts: &[(AccountId, RiskLimits)],
    ) -> Result<Self, BuildError> {
        Self::validate_capacity()?;
        let mut risk = RiskEngine::new();
        for &(account, limits) in accounts {
            risk.register_account(account, limits)
                .map_err(BuildError::Registration)?;
        }
        Ok(Self {
            gateway: Gateway::new(risk, instrument),
        })
    }

    /// Restore with the original report bound. Snapshot v1 records the gateway
    /// capacities but does not store or check `REPORTS`.
    ///
    /// # Errors
    ///
    /// Rejects zero capacities, invalid snapshots or tails, and a restored
    /// instrument that differs from the configured instrument.
    pub fn restore(
        instrument: InstrumentId,
        snapshot: &[u8],
        tail: &[u8],
    ) -> Result<Self, BuildError> {
        Self::validate_capacity()?;
        let gateway = recover_snapshot_and_tail::<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS>(
            snapshot, tail,
        )
        .map_err(BuildError::Recovery)?;
        if gateway.instrument() != instrument {
            return Err(BuildError::InstrumentMismatch);
        }
        Ok(Self { gateway })
    }

    fn validate_capacity() -> Result<(), BuildError> {
        if ACCOUNTS == 0 || RISK_ORDERS == 0 || LEVELS == 0 || ORDERS == 0 || REPORTS == 0 {
            return Err(BuildError::ZeroCapacity);
        }
        Ok(())
    }

    /// Attaches fresh queues at the next sequence of the configured gateway.
    /// Create the persistence worker before accepting commands. Dropping its
    /// reader without clean shutdown poisons admission.
    ///
    /// # Errors
    ///
    /// Rejects reused storage and event batches too small for the report bound.
    pub fn build<const BATCH: usize, const EVENTS: usize>(
        self,
        storage: &mut EngineStorage<BATCH, EVENTS>,
    ) -> Result<
        EngineParts<'_, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS, BATCH, EVENTS>,
        BuildError,
    > {
        if storage.used {
            return Err(BuildError::StorageAlreadyUsed);
        }
        let (producer, events) = storage.events.split();
        let (writer, journal) = storage.journal.split(self.gateway.expected_sequence().0);
        let status = writer
            .status_reader()
            .ok_or(BuildError::JournalStatusUnavailable)?;
        let engine =
            BoundedEventEngine::try_new(self.gateway, producer).map_err(BuildError::Events)?;
        storage.used = true;
        Ok(EngineParts {
            engine: Engine {
                events: engine,
                journal: Some(writer),
                status,
                failed: false,
            },
            events,
            journal,
        })
    }
}
