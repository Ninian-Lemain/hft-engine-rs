#![forbid(unsafe_code)]

use hft_events::{
    BoundedEventEngine, CommandKind, Event, EventBatch, EventEngineError, EventId, Rejected,
};
use hft_gateway::{Gateway, GatewayError};
use hft_journal::{JournalChannel, JournalError, JournalReader, RING_CAPACITY, ReadError};
use hft_risk::{RiskEngine, RiskLimits};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity,
    RejectReason, SequenceNumber, Side, TimeInForce,
};
use hft_wire::{encode_cancel_order, encode_new_order};

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
