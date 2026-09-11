use crate::Seed;
use hft_gateway::{Gateway, GatewayError, GatewayOutcome, GatewayState};
use hft_io::RxFrame;
use hft_journal::{JournalError, JournalRecord, RECORD_SIZE};
use hft_model::{Command as GeneratedCommand, CommandGen, GenConfig};
use hft_recovery::{
    RecoveryError, Snapshot, SnapshotError, decode_snapshot, encode_snapshot,
    recover_snapshot_and_tail,
};
use hft_risk::{RegistrationError, RiskEngine, RiskLimits};
use hft_types::{
    AccountId, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, ReportBuffer, SequenceNumber,
    Side, TimeInForce,
};
use hft_wire::{encode_cancel_order, encode_new_order, encode_replace_order};
use std::fmt;

const ACCOUNTS: usize = 4;
const RISK_ORDERS: usize = 64;
const LEVELS: usize = 8;
const ORDERS_PER_LEVEL: usize = 8;
const REPORTS: usize = 8;
const TAIL_RECORD_LIMIT: usize = 64;
const INSTRUMENT: InstrumentId = InstrumentId(7);

type SoakGateway = Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryFaultResult {
    pub steps: u64,
    pub commands: u64,
    pub business_rejections: u64,
    pub accepted_new_orders: u64,
    pub accepted_cancels: u64,
    pub accepted_replaces: u64,
    /// Successful cancellations in the second half of the requested commands.
    pub late_accepted_cancels: u64,
    /// Successful quantity changes in the second half of the requested commands.
    pub late_accepted_replaces: u64,
    /// Commands applied after installing a recovered gateway at a checkpoint.
    pub resumed_commands: u64,
    pub checkpoints: u64,
    pub fault_checks: u64,
    pub journal_peak_bytes: usize,
    pub state_digest: u64,
    pub snapshot_digest: [u8; 32],
}

#[derive(Debug)]
pub enum RecoveryFaultError {
    ZeroSteps,
    ArithmeticOverflow,
    Registration(RegistrationError),
    Journal(JournalError),
    Gateway {
        sequence: SequenceNumber,
        source: GatewayError,
    },
    Snapshot(SnapshotError),
    Recovery {
        sequence: SequenceNumber,
        stage: &'static str,
        source: RecoveryError,
    },
    Invariant(&'static str),
    AtSequence {
        sequence: SequenceNumber,
        source: Box<Self>,
    },
}

impl fmt::Display for RecoveryFaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSteps => {
                formatter.write_str("recovery fault steps must be greater than zero")
            }
            Self::ArithmeticOverflow => formatter.write_str("recovery fault counter overflow"),
            Self::Registration(source) => {
                write!(formatter, "account registration failed: {source:?}")
            }
            Self::Journal(source) => write!(formatter, "journal record failed: {source}"),
            Self::Gateway { sequence, source } => {
                write!(
                    formatter,
                    "gateway failed at sequence {}: {source:?}",
                    sequence.0
                )
            }
            Self::Snapshot(source) => write!(formatter, "snapshot failed: {source:?}"),
            Self::Recovery {
                sequence,
                stage,
                source,
            } => {
                write!(
                    formatter,
                    "recovery failed at {stage}, sequence {}: {source:?}",
                    sequence.0
                )
            }
            Self::Invariant(name) => write!(formatter, "recovery invariant failed: {name}"),
            Self::AtSequence { sequence, source } => {
                write!(
                    formatter,
                    "recovery failed at sequence {}: {source}",
                    sequence.0
                )
            }
        }
    }
}

impl std::error::Error for RecoveryFaultError {}

/// Resumes recovered gateways and compares their outcomes and full logical
/// state with an uninterrupted gateway using a bounded journal tail.
///
/// # Errors
///
/// Returns the first gateway, snapshot, journal, recovery, or equality error.
pub fn run_recovery_faults(
    seed: Seed,
    steps: u64,
) -> Result<RecoveryFaultResult, RecoveryFaultError> {
    if steps == 0 {
        return Err(RecoveryFaultError::ZeroSteps);
    }
    run_once(seed, steps)
}

fn run_once(seed: Seed, steps: u64) -> Result<RecoveryFaultResult, RecoveryFaultError> {
    let mut gateway = new_gateway()?;
    let mut uninterrupted = new_gateway()?;
    let mut generator = CommandGen::new(generator_config(), INSTRUMENT, seed.0);
    let mut reports = ReportBuffer::<REPORTS>::new();
    let mut uninterrupted_reports = ReportBuffer::<REPORTS>::new();
    let mut live_orders = LiveOrders::new();
    let mut snapshot = encode_snapshot(&gateway, 0).map_err(RecoveryFaultError::Snapshot)?;
    let mut tail = Vec::with_capacity(TAIL_RECORD_LIMIT * RECORD_SIZE);
    let mut result = RecoveryFaultResult {
        steps,
        commands: 0,
        business_rejections: 0,
        accepted_new_orders: 0,
        accepted_cancels: 0,
        accepted_replaces: 0,
        late_accepted_cancels: 0,
        late_accepted_replaces: 0,
        resumed_commands: 0,
        checkpoints: 0,
        fault_checks: check_fault_boundaries()?,
        journal_peak_bytes: 0,
        state_digest: 0,
        snapshot_digest: snapshot.digest(),
    };

    for step in 0..steps {
        let command = live_orders.direct(generator.next_command());
        let sequence = gateway.expected_sequence();
        let mut apply_step = || -> Result<(), RecoveryFaultError> {
            let frame = encode_command(command);
            if frame.sequence != sequence || uninterrupted.expected_sequence() != sequence {
                return Err(RecoveryFaultError::Invariant("generator sequence"));
            }
            if tail.len() + RECORD_SIZE > tail.capacity() {
                return Err(RecoveryFaultError::Invariant("journal tail capacity"));
            }
            let record = JournalRecord::new(sequence, frame.bytes())
                .map_err(RecoveryFaultError::Journal)?
                .encode();
            tail.extend_from_slice(&record);
            result.journal_peak_bytes = result.journal_peak_bytes.max(tail.len());

            let outcome = gateway.process_frame(&RxFrame::from_bytes(frame.bytes()), &mut reports);
            let uninterrupted_outcome = uninterrupted.process_frame(
                &RxFrame::from_bytes(frame.bytes()),
                &mut uninterrupted_reports,
            );
            if outcome != uninterrupted_outcome || reports != uninterrupted_reports {
                return Err(RecoveryFaultError::Invariant(
                    "resumed outcome or reports mismatch",
                ));
            }
            match outcome {
                Ok(outcome) => {
                    live_orders.observe(command, outcome, &reports)?;
                    record_accepted(&mut result, outcome, step >= steps / 2)?;
                }
                Err(GatewayError::Risk(_) | GatewayError::Book(_)) => {
                    increment(&mut result.business_rejections)?;
                }
                Err(source) => return Err(RecoveryFaultError::Gateway { sequence, source }),
            }
            increment(&mut result.commands)?;
            if result.checkpoints > 0 {
                increment(&mut result.resumed_commands)?;
            }

            if tail.len() == TAIL_RECORD_LIMIT * RECORD_SIZE || step + 1 == steps {
                checkpoint(
                    &mut gateway,
                    &uninterrupted,
                    &mut snapshot,
                    &mut tail,
                    &mut live_orders,
                    sequence,
                )?;
                increment(&mut result.checkpoints)?;
            }
            Ok(())
        };
        apply_step().map_err(|source| RecoveryFaultError::AtSequence {
            sequence,
            source: Box::new(source),
        })?;
    }

    result.snapshot_digest = snapshot.digest();
    result.state_digest = u64::from_be_bytes(
        result.snapshot_digest[..8]
            .try_into()
            .map_err(|_| RecoveryFaultError::Invariant("snapshot digest length"))?,
    );
    Ok(result)
}

fn checkpoint(
    gateway: &mut SoakGateway,
    uninterrupted: &SoakGateway,
    snapshot: &mut Snapshot,
    tail: &mut Vec<u8>,
    live_orders: &mut LiveOrders,
    applied_sequence: SequenceNumber,
) -> Result<(), RecoveryFaultError> {
    let recovered =
        recover_snapshot_and_tail::<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL, REPORTS>(
            snapshot.bytes(),
            tail,
        )
        .map_err(|source| RecoveryFaultError::Recovery {
            sequence: applied_sequence,
            stage: "checkpoint",
            source,
        })?;
    let state = recovered.export_state();
    if state != gateway.export_state() || state != uninterrupted.export_state() {
        return Err(RecoveryFaultError::Invariant("recovered state mismatch"));
    }
    live_orders.refresh(&state)?;
    *snapshot =
        encode_snapshot(&recovered, applied_sequence.0).map_err(RecoveryFaultError::Snapshot)?;
    // The next segment runs on reconstructed slots and indices. The mirror
    // retains its original allocation history for outcome and state comparison.
    *gateway = recovered;
    tail.clear();
    Ok(())
}

#[derive(Clone, Copy)]
struct LiveOrder {
    order_id: OrderId,
    account_id: AccountId,
    price: PriceTicks,
    quantity: Quantity,
}

struct LiveOrders {
    orders: [Option<LiveOrder>; RISK_ORDERS],
}

impl LiveOrders {
    const fn new() -> Self {
        Self {
            orders: [None; RISK_ORDERS],
        }
    }

    fn refresh(
        &mut self,
        state: &GatewayState<LEVELS, ORDERS_PER_LEVEL>,
    ) -> Result<(), RecoveryFaultError> {
        self.orders.fill(None);
        for (levels, count) in [
            (&state.book.bids, state.book.bid_level_count),
            (&state.book.asks, state.book.ask_level_count),
        ] {
            for level in levels.iter().take(count) {
                for order in level.orders.iter().take(level.order_count) {
                    self.insert(LiveOrder {
                        order_id: order.order_id,
                        account_id: order.account_id,
                        price: order.price,
                        quantity: order.quantity,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn direct(&self, command: GeneratedCommand) -> GeneratedCommand {
        let sequence = match command {
            GeneratedCommand::New(_) => return command,
            GeneratedCommand::Cancel(cancel) => cancel.sequence,
            GeneratedCommand::Replace(replace) => replace.sequence,
        };
        let start = usize::from(sequence.0.to_le_bytes()[0]) % RISK_ORDERS;
        let Some(live) = self
            .orders
            .iter()
            .cycle()
            .skip(start)
            .take(RISK_ORDERS)
            .flatten()
            .next()
        else {
            return command;
        };
        match command {
            GeneratedCommand::Cancel(mut cancel) => {
                cancel.order_id = live.order_id;
                cancel.account_id = live.account_id;
                GeneratedCommand::Cancel(cancel)
            }
            GeneratedCommand::Replace(mut replace) => {
                replace.order_id = live.order_id;
                replace.account_id = live.account_id;
                replace.price = live.price;
                // Distinct quantities count actual transitions and exercise
                // priority loss on increases.
                if replace.quantity == live.quantity {
                    replace.quantity = Quantity(if live.quantity.0 > 1 {
                        live.quantity.0 - 1
                    } else {
                        2
                    });
                }
                GeneratedCommand::Replace(replace)
            }
            GeneratedCommand::New(_) => command,
        }
    }

    fn observe(
        &mut self,
        command: GeneratedCommand,
        outcome: GatewayOutcome,
        reports: &ReportBuffer<REPORTS>,
    ) -> Result<(), RecoveryFaultError> {
        for report in reports.iter() {
            let slot = self.slot(report.maker_order_id)?;
            let Some(live) = slot.as_mut() else {
                return Err(RecoveryFaultError::Invariant("missing filled order"));
            };
            live.quantity.0 = live
                .quantity
                .0
                .checked_sub(report.quantity.0)
                .ok_or(RecoveryFaultError::Invariant("live fill quantity"))?;
            if live.quantity.0 == 0 {
                *slot = None;
            }
        }
        match (command, outcome) {
            (GeneratedCommand::New(order), GatewayOutcome::NewOrder(summary)) => {
                if summary.resting_quantity.0 > 0 {
                    self.insert(LiveOrder {
                        order_id: order.order_id,
                        account_id: order.account_id,
                        price: order.price,
                        quantity: summary.resting_quantity,
                    })?;
                }
            }
            (GeneratedCommand::Cancel(cancel), GatewayOutcome::Cancelled(_)) => {
                *self.slot(cancel.order_id)? = None;
            }
            (GeneratedCommand::Replace(replace), GatewayOutcome::Replaced(_)) => {
                *self.slot(replace.order_id)? = Some(LiveOrder {
                    order_id: replace.order_id,
                    account_id: replace.account_id,
                    price: replace.price,
                    quantity: replace.quantity,
                });
            }
            _ => return Err(RecoveryFaultError::Invariant("command outcome kind")),
        }
        Ok(())
    }

    fn insert(&mut self, order: LiveOrder) -> Result<(), RecoveryFaultError> {
        let slot = self
            .orders
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(RecoveryFaultError::Invariant("live order capacity"))?;
        *slot = Some(order);
        Ok(())
    }

    fn slot(&mut self, order_id: OrderId) -> Result<&mut Option<LiveOrder>, RecoveryFaultError> {
        self.orders
            .iter_mut()
            .find(|slot| slot.is_some_and(|order| order.order_id == order_id))
            .ok_or(RecoveryFaultError::Invariant("missing live order"))
    }
}

fn record_accepted(
    result: &mut RecoveryFaultResult,
    outcome: GatewayOutcome,
    late: bool,
) -> Result<(), RecoveryFaultError> {
    match outcome {
        GatewayOutcome::NewOrder(_) => increment(&mut result.accepted_new_orders)?,
        GatewayOutcome::Cancelled(_) => {
            increment(&mut result.accepted_cancels)?;
            if late {
                increment(&mut result.late_accepted_cancels)?;
            }
        }
        GatewayOutcome::Replaced(_) => {
            increment(&mut result.accepted_replaces)?;
            if late {
                increment(&mut result.late_accepted_replaces)?;
            }
        }
    }
    Ok(())
}

fn check_fault_boundaries() -> Result<u64, RecoveryFaultError> {
    let gateway = new_gateway()?;
    let snapshot = encode_snapshot(&gateway, 0).map_err(RecoveryFaultError::Snapshot)?;
    let sequence_one = fault_frame(1);
    let sequence_two = fault_frame(2);

    let mut corrupt = snapshot.bytes().to_vec();
    let Some(last) = corrupt.last_mut() else {
        return Err(RecoveryFaultError::Invariant("empty snapshot"));
    };
    *last ^= 1;
    if !matches!(
        decode_snapshot::<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>(&corrupt),
        Err(SnapshotError::IntegrityMismatch)
    ) {
        return Err(RecoveryFaultError::Invariant("corrupt snapshot accepted"));
    }

    let overlap = JournalRecord::new(SequenceNumber(0), &sequence_one)
        .map_err(RecoveryFaultError::Journal)?
        .encode();
    if !matches!(
        recover(&snapshot, &overlap),
        Err(RecoveryError::JournalSequence {
            expected: 1,
            received: 0
        })
    ) {
        return Err(RecoveryFaultError::Invariant("journal overlap accepted"));
    }

    let gap = JournalRecord::new(SequenceNumber(2), &sequence_one)
        .map_err(RecoveryFaultError::Journal)?
        .encode();
    if !matches!(
        recover(&snapshot, &gap),
        Err(RecoveryError::JournalSequence {
            expected: 1,
            received: 2
        })
    ) {
        return Err(RecoveryFaultError::Invariant("journal gap accepted"));
    }

    let mismatch = JournalRecord::new(SequenceNumber(1), &sequence_two)
        .map_err(RecoveryFaultError::Journal)?
        .encode();
    if !matches!(
        recover(&snapshot, &mismatch),
        Err(RecoveryError::PayloadSequence {
            journal: 1,
            payload: 2
        })
    ) {
        return Err(RecoveryFaultError::Invariant(
            "payload sequence mismatch accepted",
        ));
    }

    if !matches!(
        recover(&snapshot, &gap[..13]),
        Err(RecoveryError::TruncatedJournal { bytes: 13 })
    ) {
        return Err(RecoveryFaultError::Invariant("truncated journal accepted"));
    }

    Ok(5)
}

fn recover(snapshot: &Snapshot, tail: &[u8]) -> Result<SoakGateway, RecoveryError> {
    recover_snapshot_and_tail::<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL, REPORTS>(
        snapshot.bytes(),
        tail,
    )
}

fn new_gateway() -> Result<SoakGateway, RecoveryFaultError> {
    let mut risk = RiskEngine::new();
    let limits = RiskLimits {
        max_quantity: Quantity(40),
        max_notional: 400_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 64,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    };
    let account_count =
        u32::try_from(ACCOUNTS).map_err(|_| RecoveryFaultError::ArithmeticOverflow)?;
    for account in 1..=account_count {
        risk.register_account(AccountId(account), limits)
            .map_err(RecoveryFaultError::Registration)?;
    }
    Ok(Gateway::new(risk, INSTRUMENT))
}

const fn generator_config() -> GenConfig {
    GenConfig {
        accounts: 4,
        minimum_price: 90,
        maximum_price: 110,
        max_quantity: 20,
        cancel_probability_pct: 35,
        duplicate_id_probability_pct: 8,
        ioc_probability_pct: 10,
        fok_probability_pct: 10,
        post_only_probability_pct: 10,
        replace_probability_pct: 35,
    }
}

struct EncodedCommand {
    bytes: [u8; 46],
    len: usize,
    sequence: SequenceNumber,
}

impl EncodedCommand {
    fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn encode_command(command: GeneratedCommand) -> EncodedCommand {
    match command {
        GeneratedCommand::New(order) => EncodedCommand {
            bytes: encode_new_order(order),
            len: 46,
            sequence: order.sequence,
        },
        GeneratedCommand::Cancel(cancel) => {
            let encoded = encode_cancel_order(cancel);
            let mut bytes = [0_u8; 46];
            bytes[..encoded.len()].copy_from_slice(&encoded);
            EncodedCommand {
                bytes,
                len: encoded.len(),
                sequence: cancel.sequence,
            }
        }
        GeneratedCommand::Replace(replace) => {
            let encoded = encode_replace_order(replace);
            let mut bytes = [0_u8; 46];
            bytes[..encoded.len()].copy_from_slice(&encoded);
            EncodedCommand {
                bytes,
                len: encoded.len(),
                sequence: replace.sequence,
            }
        }
    }
}

fn fault_frame(sequence: u64) -> [u8; 46] {
    encode_new_order(NewOrder {
        order_id: OrderId(sequence),
        account_id: AccountId(1),
        instrument_id: INSTRUMENT,
        price: PriceTicks(100),
        quantity: Quantity(1),
        sequence: SequenceNumber(sequence),
        side: Side::Buy,
        time_in_force: TimeInForce::Gtc,
    })
}

fn increment(value: &mut u64) -> Result<(), RecoveryFaultError> {
    *value = value
        .checked_add(1)
        .ok_or(RecoveryFaultError::ArithmeticOverflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_is_repeatable_and_keeps_a_bounded_tail() {
        let result = run_recovery_faults(Seed(0x1234), 20_001).expect("recovery faults");
        let repeated = run_recovery_faults(Seed(0x1234), 20_001).expect("repeated recovery faults");
        assert_eq!(result, repeated);
        assert_eq!(result.commands, 20_001);
        assert_eq!(result.checkpoints, 313);
        assert_eq!(result.fault_checks, 5);
        assert!(result.journal_peak_bytes <= TAIL_RECORD_LIMIT * RECORD_SIZE);
        assert_eq!(result.resumed_commands, 20_001 - 64);
        assert_eq!(
            result.commands,
            result.business_rejections
                + result.accepted_new_orders
                + result.accepted_cancels
                + result.accepted_replaces,
        );
        assert!(result.late_accepted_cancels > 1_000, "{result:?}");
        assert!(result.late_accepted_replaces > 500, "{result:?}");
        assert!(
            result.business_rejections < result.commands / 2,
            "{result:?}"
        );
    }

    #[test]
    fn checkpoint_boundary_counts_only_subsequent_commands_as_resumed() {
        let at_boundary = run_recovery_faults(Seed(3), 64).expect("boundary run");
        assert_eq!(at_boundary.resumed_commands, 0);
        assert_eq!(at_boundary.checkpoints, 1);
        let after_boundary = run_recovery_faults(Seed(3), 65).expect("resumed run");
        assert_eq!(after_boundary.resumed_commands, 1);
        assert_eq!(after_boundary.checkpoints, 2);
    }

    #[test]
    fn checkpoint_detects_risk_state_drift_in_uninterrupted_gateway() {
        let mut gateway = new_gateway().expect("gateway");
        let mut snapshot = encode_snapshot(&gateway, 0).expect("snapshot");
        let mut drifted_state = gateway.export_state();
        drifted_state.risk.accounts[0].killed = true;
        let uninterrupted = SoakGateway::from_state(&drifted_state).expect("drifted gateway");
        let mut tail = Vec::with_capacity(TAIL_RECORD_LIMIT * RECORD_SIZE);
        let mut live_orders = LiveOrders::new();
        assert!(matches!(
            checkpoint(
                &mut gateway,
                &uninterrupted,
                &mut snapshot,
                &mut tail,
                &mut live_orders,
                SequenceNumber(0),
            ),
            Err(RecoveryFaultError::Invariant("recovered state mismatch")),
        ));
    }

    #[test]
    fn zero_steps_are_rejected() {
        assert!(matches!(
            run_recovery_faults(Seed(1), 0),
            Err(RecoveryFaultError::ZeroSteps)
        ));
    }
}
