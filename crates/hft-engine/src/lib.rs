//! Single-instrument admission, event publication, and journal lifecycle.
#![forbid(unsafe_code)]

mod builder;

pub use builder::{BuildError, EngineBuilder, EngineParts, EngineStorage};
pub use hft_events::{Event, EventBatch};
pub use hft_journal::{DurableSink, FlushPolicy, PersistenceWorker};
pub use hft_risk::RiskLimits;
pub use hft_types::{AccountId, Command, InstrumentId, SequenceNumber};

use hft_events::{BoundedEventEngine, EventEngineError};
use hft_gateway::GatewayError;
use hft_io::RxFrame;
use hft_journal::{JournalError, JournalStatus, JournalStatusReader, JournalWriter};
use hft_recovery::{Snapshot, SnapshotError, encode_snapshot};
use hft_wire::{
    ParseError, encode_cancel_order, encode_new_order, encode_replace_order, parse_message,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineError {
    Stopped,
    Failed,
    PersistenceFailed,
    UnknownInstrument(InstrumentId),
    Parse(ParseError),
    Admission(EventEngineError),
    Journal(JournalError),
    SequenceInvariant,
    Apply(EventEngineError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineState {
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Health {
    pub state: EngineState,
    pub next_sequence: SequenceNumber,
    pub journal: JournalStatus,
}

#[derive(Debug)]
pub enum CheckpointError {
    AdmissionOpen,
    PersistencePending,
    Failed,
    SequenceInvariant,
    Snapshot(SnapshotError),
}

/// Owns the only gateway mutation and journal admission path for one instrument.
/// Events acknowledge application, not durability. Persistence runs separately.
pub struct Engine<
    'storage,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const EVENTS: usize,
> {
    events:
        BoundedEventEngine<'storage, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS, BATCH, EVENTS>,
    journal: Option<JournalWriter<'storage>>,
    status: JournalStatusReader<'storage>,
    failed: bool,
}

impl<
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const EVENTS: usize,
> Engine<'_, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS, BATCH, EVENTS>
{
    #[must_use]
    pub fn instrument(&self) -> InstrumentId {
        self.events.gateway().instrument()
    }

    #[must_use]
    pub fn expected_sequence(&self) -> SequenceNumber {
        self.events.gateway().expected_sequence()
    }

    /// # Errors
    ///
    /// Parse failures change no state. Valid frames use the command admission path.
    pub fn process_frame(&mut self, frame: &RxFrame<'_>) -> Result<(), EngineError> {
        let command = parse_message(frame)
            .map_err(EngineError::Parse)?
            .to_command();
        self.apply_journaled(command, |writer| writer.enqueue(frame.bytes()))
    }

    /// Journals one command before applying it and publishing one event batch.
    ///
    /// # Errors
    ///
    /// Sequence, instrument, and queue refusals leave the command unconsumed.
    /// Retry a backpressured command unchanged. Business rejections return success
    /// and publish a rejection event. An error after journal enqueue stops admission.
    /// Persistence failure can race a command already in flight.
    pub fn process_command(&mut self, command: Command) -> Result<(), EngineError> {
        self.apply_journaled(command, |writer| match command {
            Command::NewOrder(order) => writer.enqueue(&encode_new_order(order)),
            Command::CancelOrder(cancel) => writer.enqueue(&encode_cancel_order(cancel)),
            Command::ReplaceOrder(replace) => writer.enqueue(&encode_replace_order(replace)),
        })
    }

    fn apply_journaled(
        &mut self,
        command: Command,
        enqueue: impl FnOnce(&mut JournalWriter<'_>) -> Result<SequenceNumber, JournalError>,
    ) -> Result<(), EngineError> {
        if self.failed {
            return Err(EngineError::Failed);
        }
        if self.status.is_poisoned() {
            self.fail();
            return Err(EngineError::PersistenceFailed);
        }
        if self.journal.is_none() {
            return Err(EngineError::Stopped);
        }
        if command.instrument_id() != self.instrument() {
            return Err(EngineError::UnknownInstrument(command.instrument_id()));
        }
        let expected = self.expected_sequence();
        let writer = self.journal.as_mut().ok_or(EngineError::Stopped)?;
        if writer.next_sequence() != expected.0 {
            self.fail();
            return Err(EngineError::SequenceInvariant);
        }
        let admitted = match self.events.admit(command) {
            Ok(admitted) => admitted,
            Err(error) => {
                if matches!(error, EventEngineError::Gateway(GatewayError::RiskState(_))) {
                    self.failed = true;
                    if let Some(writer) = self.journal.take() {
                        writer.close();
                    }
                }
                return Err(EngineError::Admission(error));
            }
        };
        enqueue(writer).map_err(EngineError::Journal)?;
        if let Err(error) = admitted.apply() {
            self.fail();
            return Err(EngineError::Apply(error));
        }
        Ok(())
    }

    /// Closes the journal producer after the last admitted command.
    /// The persistence worker must still drain and flush. Calling this twice is harmless.
    pub fn stop_admission(&mut self) {
        if let Some(writer) = self.journal.take() {
            writer.close();
        }
    }

    #[must_use]
    pub fn health(&self) -> Health {
        let journal = self.status.snapshot();
        let state = if self.failed || journal.poisoned {
            EngineState::Failed
        } else if self.journal.is_some() {
            EngineState::Running
        } else if journal.shutdown_complete {
            EngineState::Stopped
        } else {
            EngineState::Stopping
        };
        Health {
            state,
            next_sequence: self.expected_sequence(),
            journal,
        }
    }

    /// Encodes a cold-path state snapshot after producer closure and final flush.
    /// Event delivery is separate. Drain the event consumer before retiring its storage.
    ///
    /// # Errors
    ///
    /// Refuses open admission, pending or failed persistence, mismatched watermarks,
    /// and snapshot encoding failures. It never flushes or writes a file.
    pub fn snapshot(&self) -> Result<Snapshot, CheckpointError> {
        let health = self.health();
        if health.state == EngineState::Failed {
            return Err(CheckpointError::Failed);
        }
        if self.journal.is_some() {
            return Err(CheckpointError::AdmissionOpen);
        }
        if !health.journal.shutdown_complete {
            return Err(CheckpointError::PersistencePending);
        }
        if health.journal.next_written_sequence != health.next_sequence.0
            || health.journal.next_durable_sequence != health.next_sequence.0
        {
            return Err(CheckpointError::SequenceInvariant);
        }
        let applied = health
            .next_sequence
            .0
            .checked_sub(1)
            .ok_or(CheckpointError::SequenceInvariant)?;
        encode_snapshot(self.events.gateway(), applied).map_err(CheckpointError::Snapshot)
    }

    fn fail(&mut self) {
        self.failed = true;
        self.stop_admission();
    }
}

impl<
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const EVENTS: usize,
> Drop for Engine<'_, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS, REPORTS, BATCH, EVENTS>
{
    fn drop(&mut self) {
        self.stop_admission();
    }
}
