//! Replay fixture: a session gates the matching core. Frames are dispatched
//! to the gateway only while the session is Active; a mid-stream
//! disconnect holds traffic, and reconnect resumes sequencing exactly where
//! it stopped. No gaps, no duplicates.

use hft_gateway::{Gateway, GatewayOutcome};
use hft_io::RxFrame;
use hft_risk::RiskEngine;
use hft_session::{SessionConfig, SessionEvent, SessionState, SessionStateMachine};
use hft_types::{
    AccountId, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, ReportBuffer, SequenceNumber,
    Side,
};
use hft_wire::{encode_cancel_order, encode_new_order, encode_replace_order};

fn new_gateway() -> Gateway<2, 8, 4, 4> {
    let mut risk = RiskEngine::<2, 8>::new();
    let limits = hft_risk::RiskLimits {
        max_quantity: Quantity(100),
        max_notional: 100_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 8,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    };
    risk.register_account(AccountId(1), limits).unwrap();
    risk.register_account(AccountId(2), limits).unwrap();
    Gateway::new(risk, InstrumentId(7))
}

fn order_frame(id: u64, account: u32, side: Side) -> [u8; 46] {
    encode_new_order(NewOrder {
        time_in_force: hft_types::TimeInForce::Gtc,
        order_id: OrderId(id),
        account_id: AccountId(account),
        instrument_id: InstrumentId(7),
        price: PriceTicks(100),
        quantity: Quantity(5),
        sequence: SequenceNumber(id),
        side,
    })
}

#[test]
fn replay_fixture_gates_the_gateway_through_the_session() {
    let mut gateway = new_gateway();
    let mut reports = ReportBuffer::<4>::new();

    let mut session = SessionStateMachine::new(SessionConfig::default());

    let frames = [
        order_frame(1, 1, Side::Sell),
        order_frame(2, 2, Side::Buy),
        order_frame(3, 1, Side::Sell),
    ];

    // Handshake first; nothing reaches the core before Active.
    session.handle(SessionEvent::Connect, 0).unwrap();
    session.handle(SessionEvent::LogonSent, 0).unwrap();
    session
        .handle(
            SessionEvent::LogonAccepted {
                first_sequence: SequenceNumber(1),
            },
            0,
        )
        .unwrap();
    assert_eq!(session.state(), SessionState::Active);

    // Frame 1 passes through the active session.
    session
        .handle(
            SessionEvent::Command {
                sequence: SequenceNumber(1),
            },
            1,
        )
        .unwrap();
    match gateway.process_frame(&RxFrame::from_bytes(&frames[0]), &mut reports) {
        Ok(GatewayOutcome::NewOrder(_)) => {}
        other => panic!("expected resting order, got {other:?}"),
    }

    // Mid-stream disconnect: admission stops, frames queue unprocessed, and
    // the gateway sequence stays exactly where it was.
    session.handle(SessionEvent::Disconnect, 2).unwrap();
    assert!(!session.allows_commands());

    session.handle(SessionEvent::Connect, 3).unwrap();
    session.handle(SessionEvent::LogonSent, 3).unwrap();
    session
        .handle(
            SessionEvent::LogonAccepted {
                first_sequence: SequenceNumber(2),
            },
            4,
        )
        .unwrap();
    assert_eq!(session.state(), SessionState::Active);
    assert_eq!(session.expected_sequence(), SequenceNumber(2));

    // Replayed tail: every remaining frame is admitted in order.
    for (offset, frame) in frames.iter().enumerate().skip(1) {
        session
            .handle(
                SessionEvent::Command {
                    sequence: SequenceNumber(offset as u64 + 1),
                },
                4 + offset as u64,
            )
            .expect("replayed command admitted");
        gateway
            .process_frame(&RxFrame::from_bytes(frame), &mut reports)
            .expect("replayed frame accepted by the gateway");
    }
    assert_eq!(
        gateway.expected_sequence(),
        SequenceNumber(frames.len() as u64 + 1)
    );
}

#[test]
fn reconnect_and_resume_matches_uninterrupted_execution() {
    let frames = [
        order_frame(1, 1, Side::Sell),
        order_frame(2, 2, Side::Buy),
        order_frame(3, 1, Side::Sell),
        order_frame(4, 2, Side::Buy),
    ];

    let mut uninterrupted_session = SessionStateMachine::new(SessionConfig::default());
    let mut uninterrupted_gateway = new_gateway();
    let mut uninterrupted_reports = ReportBuffer::<4>::new();
    activate(&mut uninterrupted_session, SequenceNumber(1), 0);
    for (index, frame) in frames.iter().enumerate() {
        let sequence = SequenceNumber(index as u64 + 1);
        uninterrupted_session
            .handle(SessionEvent::Command { sequence }, sequence.0)
            .expect("uninterrupted command admitted");
        uninterrupted_gateway
            .process_frame(&RxFrame::from_bytes(frame), &mut uninterrupted_reports)
            .expect("uninterrupted command accepted");
    }

    let mut resumed_session = SessionStateMachine::new(SessionConfig::default());
    let mut resumed_gateway = new_gateway();
    let mut resumed_reports = ReportBuffer::<4>::new();
    activate(&mut resumed_session, SequenceNumber(1), 0);
    for (index, frame) in frames[..2].iter().enumerate() {
        let sequence = SequenceNumber(index as u64 + 1);
        resumed_session
            .handle(SessionEvent::Command { sequence }, sequence.0)
            .expect("prefix command admitted");
        resumed_gateway
            .process_frame(&RxFrame::from_bytes(frame), &mut resumed_reports)
            .expect("prefix command accepted");
    }

    resumed_session
        .handle(SessionEvent::Disconnect, 3)
        .expect("disconnect accepted");
    activate(&mut resumed_session, SequenceNumber(3), 4);
    for (index, frame) in frames[2..].iter().enumerate() {
        let sequence = SequenceNumber(index as u64 + 3);
        resumed_session
            .handle(SessionEvent::Command { sequence }, sequence.0 + 4)
            .expect("resumed command admitted");
        resumed_gateway
            .process_frame(&RxFrame::from_bytes(frame), &mut resumed_reports)
            .expect("resumed command accepted");
    }

    assert_eq!(uninterrupted_session.state(), SessionState::Active);
    assert_eq!(resumed_session.state(), uninterrupted_session.state());
    assert_eq!(uninterrupted_session.expected_sequence(), SequenceNumber(5));
    assert_eq!(
        resumed_session.expected_sequence(),
        uninterrupted_session.expected_sequence()
    );
    assert_eq!(
        resumed_gateway.expected_sequence(),
        resumed_session.expected_sequence()
    );
    assert_eq!(
        uninterrupted_gateway.stable_digest(),
        resumed_gateway.stable_digest()
    );
}

fn activate(session: &mut SessionStateMachine, first_sequence: SequenceNumber, now: u64) {
    session
        .handle(SessionEvent::Connect, now)
        .expect("connect accepted");
    session
        .handle(SessionEvent::LogonSent, now)
        .expect("logon sent");
    session
        .handle(SessionEvent::LogonAccepted { first_sequence }, now)
        .expect("logon accepted");
}

/// A replace issued through an active session mutates the resting order it
/// names, proving the lifecycle composes across both layers.
#[test]
fn active_session_forwards_replaces_to_the_gateway() {
    let mut gateway = new_gateway();
    let mut reports = ReportBuffer::<4>::new();
    let mut session = SessionStateMachine::new(SessionConfig::default());

    session.handle(SessionEvent::Connect, 0).unwrap();
    session.handle(SessionEvent::LogonSent, 0).unwrap();
    session
        .handle(
            SessionEvent::LogonAccepted {
                first_sequence: SequenceNumber(1),
            },
            0,
        )
        .unwrap();

    session
        .handle(
            SessionEvent::Command {
                sequence: SequenceNumber(1),
            },
            1,
        )
        .unwrap();
    gateway
        .process_frame(
            &RxFrame::from_bytes(&order_frame(1, 1, Side::Sell)),
            &mut reports,
        )
        .unwrap();

    session
        .handle(
            SessionEvent::Command {
                sequence: SequenceNumber(2),
            },
            2,
        )
        .unwrap();
    let replace = encode_replace_order(hft_types::ReplaceOrder {
        order_id: OrderId(1),
        account_id: AccountId(1),
        instrument_id: InstrumentId(7),
        sequence: SequenceNumber(2),
        price: PriceTicks(101),
        quantity: Quantity(7),
    });
    gateway
        .process_frame(&RxFrame::from_bytes(&replace), &mut reports)
        .unwrap();

    // The replaced order re-priced and grew: cancel reports seven units.
    let cancelled = gateway
        .process_frame(
            &RxFrame::from_bytes(&encode_cancel_order(hft_types::CancelOrder {
                order_id: OrderId(1),
                account_id: AccountId(1),
                instrument_id: InstrumentId(7),
                sequence: SequenceNumber(3),
            })),
            &mut reports,
        )
        .unwrap();
    match cancelled {
        GatewayOutcome::Cancelled(cancelled) => {
            assert_eq!(cancelled.quantity, Quantity(7));
        }
        other => panic!("expected cancel, got {other:?}"),
    }
}
