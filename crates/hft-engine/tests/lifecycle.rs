#![forbid(unsafe_code)]

use hft_engine::{
    BuildError, CheckpointError, DurableSink, EngineBuilder, EngineError, EngineParts, EngineState,
    EngineStorage, Event, FlushPolicy, PersistenceWorker, RiskLimits,
};
use hft_events::EventEngineError;
use hft_gateway::{Gateway, GatewayError};
use hft_io::RxFrame;
use hft_journal::{JournalError, JournalRecord, PersistError, RECORD_SIZE, RING_CAPACITY};
use hft_recovery::{decode_snapshot, encode_snapshot};
use hft_risk::{RegistrationError, RiskEngine};
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity,
    RejectReason, ReplaceOrder, SequenceNumber, Side, TimeInForce,
};
use hft_wire::{encode_new_order, parse_message};
use std::io;

type Builder = EngineBuilder<2, 16, 4, 4, 4>;
const INSTRUMENT: InstrumentId = InstrumentId(7);

fn limits() -> RiskLimits {
    RiskLimits {
        max_quantity: Quantity(100),
        max_notional: 100_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 16,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    }
}

fn builder() -> Builder {
    Builder::new(INSTRUMENT, &[(AccountId(1), limits())]).expect("builder")
}

fn order(sequence: u64, quantity: u64) -> NewOrder {
    NewOrder {
        order_id: OrderId(sequence),
        account_id: AccountId(1),
        instrument_id: INSTRUMENT,
        price: PriceTicks(100),
        quantity: Quantity(quantity),
        sequence: SequenceNumber(sequence),
        side: Side::Buy,
        time_in_force: TimeInForce::Gtc,
    }
}

fn unknown_cancel(sequence: u64) -> Command {
    Command::CancelOrder(CancelOrder {
        order_id: OrderId(sequence),
        account_id: AccountId(1),
        instrument_id: INSTRUMENT,
        sequence: SequenceNumber(sequence),
    })
}

#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    flushes: usize,
    fail_write: bool,
    fail_flush: bool,
}

impl DurableSink for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_write {
            return Err(io::ErrorKind::Other.into());
        }
        let count = bytes.len().min(7);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::ErrorKind::Other.into());
        }
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn configuration_rejects_invalid_accounts_capacities_and_reused_storage() {
    assert!(matches!(
        Builder::new(INSTRUMENT, &[(AccountId(1), limits()); 2]),
        Err(BuildError::Registration(
            RegistrationError::DuplicateAccount
        ))
    ));
    assert!(matches!(
        EngineBuilder::<0, 16, 4, 4, 4>::new(INSTRUMENT, &[]),
        Err(BuildError::ZeroCapacity)
    ));
    let mut invalid = limits();
    invalid.max_quantity = Quantity(0);
    assert!(matches!(
        Builder::new(INSTRUMENT, &[(AccountId(1), invalid)]),
        Err(BuildError::Registration(RegistrationError::InvalidLimits))
    ));
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    assert!(EngineStorage::<6, 3>::try_new().is_err());
    let mut short = EngineStorage::<5, 2>::try_new().expect("storage");
    assert!(matches!(
        builder().build(&mut short),
        Err(BuildError::Events(_))
    ));
    let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
    drop(builder().build(&mut storage).expect("parts"));
    assert!(matches!(
        builder().build(&mut storage),
        Err(BuildError::StorageAlreadyUsed)
    ));
}

#[test]
fn frame_sequence_and_instrument_refusals_consume_nothing() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = builder().build(&mut storage).expect("parts");
    let mut worker =
        PersistenceWorker::<_, 2>::new(journal, Sink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    assert!(matches!(
        engine.process_frame(&RxFrame::from_bytes(&[])),
        Err(EngineError::Parse(_))
    ));
    assert!(matches!(
        engine.process_command(Command::NewOrder(order(2, 1))),
        Err(EngineError::Admission(EventEngineError::Gateway(
            GatewayError::Sequence { .. }
        )))
    ));
    let mut wrong = order(1, 1);
    wrong.instrument_id = InstrumentId(8);
    assert_eq!(
        engine.process_command(Command::NewOrder(wrong)),
        Err(EngineError::UnknownInstrument(InstrumentId(8)))
    );
    assert_eq!(engine.expected_sequence(), SequenceNumber(1));
    assert!(events.try_pop().is_none());
    assert_eq!(worker.drain_batch().expect("empty"), 0);
    engine
        .process_frame(&RxFrame::from_bytes(&encode_new_order(order(1, 2))))
        .expect("frame");
    assert!(events.try_pop().is_some());
    engine.stop_admission();
    worker.shutdown().expect("shutdown");
    let sink = worker.into_sink().expect("sink");
    assert_eq!(sink.bytes.len(), RECORD_SIZE);
    let record = JournalRecord::decode(sink.bytes.as_slice().try_into().expect("record size"))
        .expect("record");
    assert_eq!(record.sequence(), SequenceNumber(1));
    assert_eq!(
        record.slice(),
        Some(encode_new_order(order(1, 2)).as_slice())
    );
}

#[test]
fn event_pressure_and_journal_pressure_leave_one_retry() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = builder().build(&mut storage).expect("parts");
    let mut worker =
        PersistenceWorker::<_, 1>::new(journal, Sink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    for sequence in 1..=2 {
        engine
            .process_command(unknown_cancel(sequence))
            .expect("rejection");
    }
    assert_eq!(
        engine.process_command(unknown_cancel(3)),
        Err(EngineError::Admission(EventEngineError::Backpressured))
    );
    assert_eq!(engine.expected_sequence(), SequenceNumber(3));
    for sequence in 1..=2 {
        let batch = events.try_pop().expect("batch");
        assert!(
            matches!(batch.iter().next(), Some(Event::Rejected(rejected)) if rejected.id.command_sequence.0 == sequence)
        );
    }
    let capacity = u64::try_from(RING_CAPACITY).expect("capacity");
    for sequence in 3..=capacity {
        engine
            .process_command(unknown_cancel(sequence))
            .expect("rejection");
        assert!(events.try_pop().is_some());
    }
    let pending = unknown_cancel(capacity + 1);
    assert_eq!(
        engine.process_command(pending),
        Err(EngineError::Journal(JournalError::Saturated))
    );
    assert_eq!(engine.expected_sequence(), SequenceNumber(capacity + 1));
    assert!(events.try_pop().is_none());
    assert_eq!(worker.drain_batch().expect("reclaim"), 1);
    engine.process_command(pending).expect("retry");
    assert!(events.try_pop().is_some());
    assert!(events.try_pop().is_none());
    engine.stop_admission();
    worker.shutdown().expect("shutdown");
    let sink = worker.into_sink().expect("sink");
    assert_eq!(sink.bytes.len(), (RING_CAPACITY + 1) * RECORD_SIZE);
    for (index, bytes) in sink.bytes.chunks_exact(RECORD_SIZE).enumerate() {
        let sequence = u64::try_from(index).expect("index") + 1;
        let record = JournalRecord::decode(bytes.try_into().expect("record size")).expect("record");
        let command = parse_message(&RxFrame::from_bytes(record.slice().expect("payload")))
            .expect("wire")
            .to_command();
        assert_eq!(record.sequence(), SequenceNumber(sequence));
        assert_eq!(command, unknown_cancel(sequence));
    }
}

#[test]
fn worker_failure_and_abandonment_close_admission() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    for fault in 0..3 {
        let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
        let EngineParts {
            mut engine,
            mut events,
            journal,
        } = builder().build(&mut storage).expect("parts");
        engine
            .process_command(Command::NewOrder(order(1, 1)))
            .expect("first");
        events.try_pop().expect("first event");
        let mut worker = PersistenceWorker::<_, 1>::new(
            journal,
            Sink {
                fail_write: fault == 0,
                fail_flush: fault == 1,
                ..Sink::default()
            },
            FlushPolicy::EveryBatch,
        )
        .expect("worker");
        if fault < 2 {
            assert!(matches!(worker.drain_batch(), Err(PersistError::Io(_))));
        }
        drop(worker);
        assert_eq!(engine.health().state, EngineState::Failed);
        assert_eq!(
            engine.process_command(Command::NewOrder(order(2, 1))),
            Err(EngineError::PersistenceFailed)
        );
        assert_eq!(
            engine.process_command(Command::NewOrder(order(2, 1))),
            Err(EngineError::Failed)
        );
        assert_eq!(engine.expected_sequence(), SequenceNumber(2));
        assert!(engine.health().journal.producer_closed);
        assert!(events.try_pop().is_none());
        assert!(matches!(engine.snapshot(), Err(CheckpointError::Failed)));
    }
}

#[test]
fn shutdown_snapshot_and_restart_preserve_the_next_sequence() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut storage = EngineStorage::<6, 4>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = builder().build(&mut storage).expect("parts");
    let mut worker =
        PersistenceWorker::<_, 2>::new(journal, Sink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    assert!(matches!(
        engine.snapshot(),
        Err(CheckpointError::AdmissionOpen)
    ));
    assert!(matches!(worker.shutdown(), Err(PersistError::ProducerOpen)));
    engine
        .process_command(Command::NewOrder(order(1, 4)))
        .expect("new");
    engine
        .process_command(Command::ReplaceOrder(ReplaceOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: INSTRUMENT,
            sequence: SequenceNumber(2),
            price: PriceTicks(100),
            quantity: Quantity(2),
        }))
        .expect("replace");
    engine
        .process_command(Command::NewOrder(order(3, 101)))
        .expect("business rejection");
    assert_eq!(worker.drain_batch().expect("write"), 2);
    assert_eq!(engine.health().journal.next_durable_sequence, 1);
    engine.stop_admission();
    engine.stop_admission();
    assert_eq!(engine.health().state, EngineState::Stopping);
    assert_eq!(
        engine.process_command(unknown_cancel(4)),
        Err(EngineError::Stopped)
    );
    assert!(matches!(
        engine.snapshot(),
        Err(CheckpointError::PersistencePending)
    ));
    worker.shutdown().expect("shutdown");
    worker.shutdown().expect("idempotent shutdown");
    assert_eq!(engine.health().state, EngineState::Stopped);
    let snapshot = engine.snapshot().expect("snapshot");
    assert_eq!(snapshot.applied_sequence(), 3);
    let restored = decode_snapshot::<2, 16, 4, 4>(snapshot.bytes()).expect("decode");
    assert_eq!(restored.gateway.expected_sequence(), SequenceNumber(4));
    assert_eq!(
        restored
            .gateway
            .top_level(Side::Buy)
            .expect("bid")
            .aggregate_quantity,
        2
    );
    let mut seen = 0;
    while events.try_pop().is_some() {
        seen += 1;
    }
    assert_eq!(seen, 3);
    let sink = worker.into_sink().expect("sink");
    assert_eq!(sink.flushes, 1);
    assert!(matches!(
        Builder::restore(InstrumentId(8), snapshot.bytes(), &[]),
        Err(BuildError::InstrumentMismatch)
    ));
    assert_restart(snapshot.bytes(), &[]);
    let mut risk = RiskEngine::<2, 16>::new();
    risk.register_account(AccountId(1), limits())
        .expect("account");
    let initial = encode_snapshot(&Gateway::<2, 16, 4, 4>::new(risk, INSTRUMENT), 0)
        .expect("initial snapshot");
    assert_restart(initial.bytes(), &sink.bytes);
}

fn assert_restart(snapshot: &[u8], tail: &[u8]) {
    let mut resumed_storage = EngineStorage::<6, 2>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = Builder::restore(INSTRUMENT, snapshot, tail)
        .expect("restore")
        .build(&mut resumed_storage)
        .expect("resume");
    let mut resumed =
        PersistenceWorker::<_, 1>::new(journal, Sink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    assert_eq!(engine.expected_sequence(), SequenceNumber(4));
    engine
        .process_command(Command::CancelOrder(CancelOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: INSTRUMENT,
            sequence: SequenceNumber(4),
        }))
        .expect("cancel restored order");
    assert!(matches!(
        events.try_pop().expect("event").iter().next(),
        Some(Event::Cancelled(_))
    ));
    drop(engine);
    resumed.shutdown().expect("drop closed producer");
    let sink = resumed.into_sink().expect("sink");
    assert_eq!(
        JournalRecord::decode(sink.bytes.as_slice().try_into().expect("record size"))
            .expect("record")
            .sequence(),
        SequenceNumber(4)
    );
}

#[test]
fn sequence_exhaustion_stops_without_journaling() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut risk = RiskEngine::<2, 16>::new();
    risk.register_account(AccountId(1), limits())
        .expect("account");
    let mut state = Gateway::<2, 16, 4, 4>::new(risk, INSTRUMENT).export_state();
    state.expected_sequence = SequenceNumber(u64::MAX);
    let gateway = Gateway::<2, 16, 4, 4>::from_state(&state).expect("state");
    let snapshot = encode_snapshot(&gateway, u64::MAX - 1).expect("snapshot");
    let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = Builder::restore(INSTRUMENT, snapshot.bytes(), &[])
        .expect("restore")
        .build(&mut storage)
        .expect("parts");
    let mut worker =
        PersistenceWorker::<_, 1>::new(journal, Sink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    assert_eq!(
        engine.process_command(unknown_cancel(u64::MAX)),
        Err(EngineError::Admission(EventEngineError::Gateway(
            GatewayError::RiskState(RejectReason::ArithmeticOverflow)
        )))
    );
    assert_eq!(engine.health().state, EngineState::Failed);
    assert!(events.try_pop().is_none());
    worker.shutdown().expect("empty shutdown");
    assert!(worker.into_sink().expect("sink").bytes.is_empty());
}

#[test]
fn separate_workers_drain_the_last_command_after_close() {
    const COMMANDS: u64 = 4_096;
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut storage = EngineStorage::<6, 2>::try_new().expect("storage");
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = builder().build(&mut storage).expect("parts");
    std::thread::scope(|scope| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let persistence = scope.spawn(move || {
            let mut worker =
                PersistenceWorker::<_, 7>::new(journal, Sink::default(), FlushPolicy::EveryBatch)
                    .expect("worker");
            loop {
                assert!(std::time::Instant::now() < deadline, "persistence deadline");
                worker.drain_batch().expect("batch");
                match worker.shutdown() {
                    Ok(()) => break,
                    Err(PersistError::ProducerOpen) => std::thread::yield_now(),
                    Err(error) => panic!("shutdown failed: {error:?}"),
                }
            }
            worker.into_sink().expect("sink")
        });
        let publication = scope.spawn(move || {
            for expected in 1..=COMMANDS {
                loop {
                    assert!(std::time::Instant::now() < deadline, "event deadline");
                    if let Some(batch) = events.try_pop() {
                        assert_eq!(batch.len(), 1);
                        assert!(
                            matches!(batch.iter().next(), Some(Event::Rejected(rejected))
                            if rejected.id.command_sequence.0 == expected)
                        );
                        break;
                    }
                    std::thread::yield_now();
                }
            }
            assert!(events.try_pop().is_none());
        });
        for sequence in 1..=COMMANDS {
            loop {
                assert!(std::time::Instant::now() < deadline, "admission deadline");
                match engine.process_command(unknown_cancel(sequence)) {
                    Ok(()) => break,
                    Err(
                        EngineError::Admission(EventEngineError::Backpressured)
                        | EngineError::Journal(JournalError::Saturated),
                    ) => std::thread::yield_now(),
                    Err(error) => panic!("admission failed: {error:?}"),
                }
            }
        }
        engine.stop_admission();
        let sink = persistence.join().expect("persistence");
        publication.join().expect("publication");
        assert_eq!(
            sink.bytes.len(),
            usize::try_from(COMMANDS).expect("commands") * RECORD_SIZE
        );
        assert_eq!(engine.health().state, EngineState::Stopped);
        assert_eq!(engine.health().journal.next_durable_sequence, COMMANDS + 1);
        assert_eq!(
            engine.snapshot().expect("snapshot").applied_sequence(),
            COMMANDS
        );
    });
}
