#![forbid(unsafe_code)]

use hft_events::{
    BoundedEventEngine, CommandKind, Event, EventBatch, EventEngineError, EventId, Rejected,
};
use hft_gateway::{Gateway, GatewayError};
use hft_journal::{
    DurableSink, FlushPolicy, JournalChannel, JournalError, JournalReader, PersistError,
    PersistenceWorker, RECORD_SIZE, RING_CAPACITY, ReadError,
};
use hft_recovery::{encode_snapshot, recover_snapshot_and_tail};
use hft_risk::{RiskEngine, RiskLimits};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity,
    RejectReason, SequenceNumber, Side, TimeInForce,
};
use hft_wire::{encode_cancel_order, encode_new_order};
use std::{cell::RefCell, io, rc::Rc};

type TestGateway = Gateway<1, 8, 4, 4>;
type TestBatch = EventBatch<6>;
type TestEngine<'queue> = BoundedEventEngine<'queue, 1, 8, 4, 4, 4, 6, 1>;

fn gateway() -> TestGateway {
    let mut risk = RiskEngine::new();
    risk.register_account(
        AccountId(1),
        RiskLimits {
            max_quantity: Quantity(100),
            max_notional: 100_000,
            max_abs_position: Quantity(1_000),
            max_open_orders: 8,
            minimum_price: PriceTicks(1),
            maximum_price: PriceTicks(1_000),
        },
    )
    .expect("register account");
    Gateway::new(risk, InstrumentId(7))
}

fn order(sequence: u64, quantity: u64) -> NewOrder {
    NewOrder {
        order_id: OrderId(sequence),
        account_id: AccountId(1),
        instrument_id: InstrumentId(7),
        price: PriceTicks(100),
        quantity: Quantity(quantity),
        sequence: SequenceNumber(sequence),
        side: Side::Buy,
        time_in_force: TimeInForce::Gtc,
    }
}

fn cancel(sequence: u64) -> CancelOrder {
    CancelOrder {
        order_id: OrderId(sequence),
        account_id: AccountId(1),
        instrument_id: InstrumentId(7),
        sequence: SequenceNumber(sequence),
    }
}

fn assert_record(reader: &mut JournalReader<'_>, sequence: u64, frame: &[u8]) {
    let record = reader.read().expect("journal record");
    assert_eq!(record.sequence(), SequenceNumber(sequence));
    assert_eq!(record.slice(), frame);
}

#[test]
fn full_event_queue_leaves_journal_and_gateway_unchanged() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
    let (producer, mut consumer) = events.split();
    let mut engine = TestEngine::try_new(gateway(), producer).expect("event engine");
    let mut journal = JournalChannel::try_new().expect("journal channel");
    let (mut writer, mut reader) = journal.split(1);

    let first = order(1, 2);
    let first_frame = encode_new_order(first);
    assert_eq!(writer.next_sequence(), first.sequence.0);
    let admitted = engine.admit(Command::NewOrder(first)).expect("admission");
    assert_eq!(writer.enqueue(&first_frame), Ok(first.sequence));
    admitted.apply().expect("first command");

    let before = engine.gateway().export_state();
    let second = order(2, 3);
    let second_frame = encode_new_order(second);
    assert_eq!(writer.next_sequence(), second.sequence.0);
    assert!(matches!(
        engine.admit(Command::NewOrder(second)),
        Err(EventEngineError::Backpressured)
    ));
    assert_eq!(engine.gateway().export_state(), before);
    assert_eq!(writer.next_sequence(), 2);
    assert_record(&mut reader, 1, &first_frame);
    assert!(matches!(reader.read(), Err(ReadError::Empty)));

    let first_batch = consumer.try_pop().expect("first event batch");
    assert!(matches!(
        first_batch.iter().next(),
        Some(Event::Accepted(_))
    ));
    assert!(consumer.try_pop().is_none());
    let admitted = engine
        .admit(Command::NewOrder(second))
        .expect("retry admission");
    assert_eq!(writer.enqueue(&second_frame), Ok(second.sequence));
    admitted.apply().expect("second command");
    assert_eq!(writer.next_sequence(), 3);
    assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(3));
    assert_record(&mut reader, 2, &second_frame);
    assert!(matches!(reader.read(), Err(ReadError::Empty)));
    assert!(consumer.try_pop().is_some());
    assert!(consumer.try_pop().is_none());
}

#[test]
fn full_journal_drops_admission_and_reclaimed_slot_accepts_one_retry() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
    let (producer, mut consumer) = events.split();
    let mut engine = TestEngine::try_new(gateway(), producer).expect("event engine");
    let mut journal = JournalChannel::try_new().expect("journal channel");
    let (mut writer, mut reader) = journal.split(1);
    let capacity = u64::try_from(RING_CAPACITY).expect("journal capacity fits sequence");

    for sequence in 1..=capacity {
        let command = cancel(sequence);
        assert_eq!(writer.next_sequence(), command.sequence.0);
        let admitted = engine
            .admit(Command::CancelOrder(command))
            .expect("admission");
        assert_eq!(
            writer.enqueue(&encode_cancel_order(command)),
            Ok(command.sequence)
        );
        admitted.apply().expect("publish cancellation rejection");
        let batch = consumer.try_pop().expect("rejection batch");
        assert_eq!(batch.len(), 1);
        assert!(matches!(
            batch.iter().next(),
            Some(Event::Rejected(Rejected {
                id: EventId { command_sequence, ordinal: 0 },
                reason: RejectReason::UnknownOrder,
                ..
            })) if command_sequence.0 == sequence
        ));
    }

    let command = cancel(capacity + 1);
    let frame = encode_cancel_order(command);
    let before = engine.gateway().export_state();
    assert_eq!(writer.next_sequence(), command.sequence.0);
    {
        let _admitted = engine
            .admit(Command::CancelOrder(command))
            .expect("event admission");
        assert_eq!(writer.enqueue(&frame), Err(JournalError::Saturated));
        assert!(consumer.try_pop().is_none());
    }
    assert_eq!(engine.gateway().export_state(), before);
    assert_eq!(writer.next_sequence(), command.sequence.0);
    assert!(consumer.try_pop().is_none());

    assert_record(&mut reader, 1, &encode_cancel_order(cancel(1)));
    let admitted = engine
        .admit(Command::CancelOrder(command))
        .expect("retry admission");
    assert_eq!(writer.enqueue(&frame), Ok(command.sequence));
    admitted.apply().expect("apply retry");
    assert_eq!(writer.next_sequence(), capacity + 2);
    assert_eq!(
        engine.gateway().expected_sequence(),
        SequenceNumber(capacity + 2)
    );
    let batch = consumer.try_pop().expect("retry event batch");
    assert_eq!(batch.len(), 1);
    assert!(matches!(
        batch.iter().next(),
        Some(Event::Rejected(Rejected {
            id: EventId { command_sequence, ordinal: 0 },
            reason: RejectReason::UnknownOrder,
            ..
        })) if *command_sequence == command.sequence
    ));
    assert!(consumer.try_pop().is_none());

    for sequence in 2..=capacity + 1 {
        assert_record(
            &mut reader,
            sequence,
            &encode_cancel_order(cancel(sequence)),
        );
    }
    assert!(matches!(reader.read(), Err(ReadError::Empty)));
    assert_eq!(reader.expected_sequence(), capacity + 2);
}

#[test]
fn business_rejection_has_matching_journal_and_event_sequences() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
    let (producer, mut consumer) = events.split();
    let mut engine = TestEngine::try_new(gateway(), producer).expect("event engine");
    let mut journal = JournalChannel::try_new().expect("journal channel");
    let (mut writer, mut reader) = journal.split(1);
    let command = order(1, 101);
    let frame = encode_new_order(command);

    assert_eq!(writer.next_sequence(), command.sequence.0);
    let admitted = engine.admit(Command::NewOrder(command)).expect("admission");
    assert_eq!(writer.enqueue(&frame), Ok(command.sequence));
    assert!(consumer.try_pop().is_none());
    admitted.apply().expect("publish rejection");

    assert_eq!(writer.next_sequence(), 2);
    assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(2));
    assert_record(&mut reader, 1, &frame);
    assert!(matches!(reader.read(), Err(ReadError::Empty)));
    let batch = consumer.try_pop().expect("rejection batch");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch.iter().next(),
        Some(&Event::Rejected(Rejected {
            id: EventId {
                command_sequence: SequenceNumber(1),
                ordinal: 0
            },
            command: CommandKind::NewOrder,
            order_id: command.order_id,
            account_id: command.account_id,
            instrument_id: command.instrument_id,
            reason: RejectReason::QuantityLimit,
        }))
    );
    assert!(consumer.try_pop().is_none());
}

#[test]
fn sequence_failures_happen_before_journal_enqueue() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    for (expected, received, error) in [
        (
            1,
            2,
            GatewayError::Sequence {
                expected: SequenceNumber(1),
                received: SequenceNumber(2),
            },
        ),
        (
            u64::MAX,
            u64::MAX,
            GatewayError::RiskState(RejectReason::ArithmeticOverflow),
        ),
    ] {
        let mut state = gateway().export_state();
        state.expected_sequence = SequenceNumber(expected);
        let restored = TestGateway::from_state(&state).expect("sequence boundary state");
        let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
        let (producer, mut consumer) = events.split();
        let mut engine = TestEngine::try_new(restored, producer).expect("event engine");
        let mut journal = JournalChannel::try_new().expect("journal channel");
        let (writer, mut reader) = journal.split(expected);

        assert_eq!(
            writer.next_sequence(),
            engine.gateway().expected_sequence().0
        );
        assert!(matches!(
            engine.admit(Command::NewOrder(order(received, 1))),
            Err(EventEngineError::Gateway(actual)) if actual == error
        ));
        assert_eq!(engine.gateway().export_state(), state);
        assert_eq!(writer.next_sequence(), expected);
        assert_eq!(reader.expected_sequence(), expected);
        assert!(matches!(reader.read(), Err(ReadError::Empty)));
        assert!(consumer.try_pop().is_none());
    }
}

struct Storage {
    bytes: [u8; 2 * RECORD_SIZE],
    written: usize,
    durable: usize,
    fail_flush: bool,
}

struct CrashSink(Rc<RefCell<Storage>>);

impl DurableSink for CrashSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut storage = self.0.borrow_mut();
        let start = storage.written;
        let count = bytes.len().min(7).min(storage.bytes.len() - start);
        storage.bytes[start..start + count].copy_from_slice(&bytes[..count]);
        storage.written += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut storage = self.0.borrow_mut();
        if storage.fail_flush {
            return Err(io::ErrorKind::Other.into());
        }
        storage.durable = storage.written;
        Ok(())
    }
}

#[test]
fn recovery_uses_the_durable_prefix_after_a_flush_failure() {
    persistence_recovery(true);
}

#[test]
fn clean_shutdown_makes_the_published_tail_recoverable() {
    persistence_recovery(false);
}

fn persistence_recovery(fail_flush: bool) {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
    let (producer, mut consumer) = events.split();
    let initial = gateway();
    let snapshot = encode_snapshot(&initial, 0).expect("initial snapshot");
    let mut engine = TestEngine::try_new(initial, producer).expect("event engine");
    let mut journal = JournalChannel::try_new().expect("journal channel");
    let (mut writer, reader) = journal.split(1);
    let status = writer.status_reader().expect("controlled status");
    let storage = Rc::new(RefCell::new(Storage {
        bytes: [0; 2 * RECORD_SIZE],
        written: 0,
        durable: 0,
        fail_flush: false,
    }));
    let mut worker = PersistenceWorker::<_, 1>::new(
        reader,
        CrashSink(Rc::clone(&storage)),
        FlushPolicy::EveryBatch,
    )
    .expect("worker");

    let mut durable_state = engine.gateway().export_state();
    for sequence in 1..=2 {
        let command = order(sequence, 2);
        assert_eq!(writer.next_sequence(), sequence);
        let admitted = engine.admit(Command::NewOrder(command)).expect("admission");
        writer.enqueue(&encode_new_order(command)).expect("journal");
        admitted.apply().expect("apply");
        let batch = consumer.try_pop().expect("published batch");
        assert!(matches!(batch.iter().next(), Some(Event::Accepted(_))));
        assert_eq!(status.snapshot().next_durable_sequence, sequence);
        assert_eq!(engine.gateway().expected_sequence().0, sequence + 1);

        if sequence == 1 {
            assert_eq!(worker.drain_batch().expect("first batch"), 1);
            durable_state = engine.gateway().export_state();
            assert_eq!(status.snapshot().next_durable_sequence, 2);
        }
    }
    writer.close();
    storage.borrow_mut().fail_flush = fail_flush;
    if fail_flush {
        assert!(matches!(worker.shutdown(), Err(PersistError::Io(_))));
    } else {
        worker.shutdown().expect("shutdown");
        durable_state = engine.gateway().export_state();
    }
    let progress = status.snapshot();
    assert_eq!(progress.next_written_sequence, 3);
    assert_eq!(
        progress.next_durable_sequence,
        if fail_flush { 2 } else { 3 }
    );
    assert_eq!(progress.poisoned, fail_flush);
    assert_eq!(progress.shutdown_complete, !fail_flush);
    assert!(progress.producer_closed);
    assert!(consumer.try_pop().is_none());

    let stored = storage.borrow();
    assert_eq!(stored.written, 2 * RECORD_SIZE);
    assert_eq!(
        stored.durable,
        if fail_flush {
            RECORD_SIZE
        } else {
            2 * RECORD_SIZE
        }
    );
    let restored = recover_snapshot_and_tail::<1, 8, 4, 4, 4>(
        snapshot.bytes(),
        &stored.bytes[..stored.durable],
    )
    .expect("recover durable bytes");
    assert_eq!(restored.export_state(), durable_state);
    if fail_flush {
        assert_ne!(restored.export_state(), engine.gateway().export_state());
    }
}
