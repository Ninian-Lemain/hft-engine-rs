use hft_book::OrderBook;
use hft_events::{BoundedEventEngine, EventBatch, EventEngineError};
use hft_gateway::{Gateway, GatewayError, GatewayOutcome};
use hft_risk::{RegistrationError, RiskEngine, RiskLimits};
use hft_router::{InstrumentRoute, MultiInstrumentRouter, RouteTable, RouterError, ShardId};
use hft_session::retransmit::{RetainError, RetransmitBuffer};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity,
    RejectReason, ReportBuffer, SequenceNumber, Side, TimeInForce,
};
use std::fmt;

const INSTRUMENT: InstrumentId = InstrumentId(11);
const ACCOUNT: AccountId = AccountId(7);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityResult {
    pub price_level_order_refusals: u64,
    pub price_level_order_retries: u64,
    pub price_level_refusals: u64,
    pub price_level_retries: u64,
    pub risk_order_refusals: u64,
    pub risk_order_retries: u64,
    pub account_registration_refusals: u64,
    pub report_refusals: u64,
    pub report_retries: u64,
    pub retransmit_refusals: u64,
    pub retransmit_retries: u64,
    pub command_queue_refusals: u64,
    pub command_queue_retries: u64,
    pub event_queue_refusals: u64,
    pub event_queue_retries: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapacityCheck {
    PriceLevelOrderSetup,
    PriceLevelOrderRefusal,
    PriceLevelOrderCancel,
    PriceLevelOrderRetry,
    PriceLevelSetup,
    PriceLevelRefusal,
    PriceLevelCancel,
    PriceLevelRetry,
    RiskAccount,
    RiskSetup,
    RiskRefusal,
    RiskRelease,
    RiskRetry,
    AccountSetup,
    AccountRefusal,
    AccountState,
    AccountRelease,
    AccountCapacityAfterRelease,
    AccountReuse,
    ReportSetup,
    ReportRefusal,
    ReportState,
    ReportCancel,
    ReportRetry,
    RetransmitSetup,
    RetransmitRefusal,
    RetransmitRelease,
    RetransmitRetry,
    CommandQueue,
    CommandRoute,
    CommandRefusal,
    CommandRelease,
    CommandRetry,
    EventQueue,
    EventEngine,
    EventProcess,
    EventRefusal,
    EventState,
    EventRelease,
    EventRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityError {
    check: CapacityCheck,
}

impl CapacityError {
    const fn at(check: CapacityCheck) -> Self {
        Self { check }
    }
}

impl fmt::Display for CapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "capacity check failed at {:?}", self.check)
    }
}

impl std::error::Error for CapacityError {}

/// Forces capacity failures and retries after reclamation where supported.
///
/// # Errors
///
/// Returns the first failed scenario check.
pub fn run_capacity() -> Result<CapacityResult, CapacityError> {
    run_price_level_order_capacity()?;
    run_price_level_capacity()?;
    run_risk_order_capacity()?;
    run_account_capacity()?;
    run_report_capacity()?;
    run_retransmit_capacity()?;
    run_command_queue_capacity()?;
    run_event_queue_capacity()?;
    Ok(CapacityResult {
        price_level_order_refusals: 1,
        price_level_order_retries: 1,
        price_level_refusals: 1,
        price_level_retries: 1,
        risk_order_refusals: 1,
        risk_order_retries: 1,
        account_registration_refusals: 2,
        report_refusals: 1,
        report_retries: 1,
        retransmit_refusals: 1,
        retransmit_retries: 1,
        command_queue_refusals: 1,
        command_queue_retries: 1,
        event_queue_refusals: 1,
        event_queue_retries: 1,
    })
}

fn run_price_level_order_capacity() -> Result<(), CapacityError> {
    let mut book = OrderBook::<1, 1>::new(INSTRUMENT);
    let first = order(1, 100, 1);
    let retry = order(2, 100, 2);
    let mut reports = ReportBuffer::<1>::new();
    book.submit(first, &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelOrderSetup))?;
    reports.clear();
    if book.submit(retry, &mut reports) != Err(RejectReason::PriceLevelOrderCapacity) {
        return Err(CapacityError::at(CapacityCheck::PriceLevelOrderRefusal));
    }
    book.cancel(cancel(first, 3))
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelOrderCancel))?;
    book.submit(retry, &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelOrderRetry))?;
    Ok(())
}

fn run_price_level_capacity() -> Result<(), CapacityError> {
    let mut book = OrderBook::<1, 1>::new(INSTRUMENT);
    let first = order(1, 100, 1);
    let retry = order(2, 101, 2);
    let mut reports = ReportBuffer::<1>::new();
    book.submit(first, &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelSetup))?;
    reports.clear();
    if book.submit(retry, &mut reports) != Err(RejectReason::PriceLevelCapacity) {
        return Err(CapacityError::at(CapacityCheck::PriceLevelRefusal));
    }
    book.cancel(cancel(first, 3))
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelCancel))?;
    book.submit(retry, &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::PriceLevelRetry))?;
    Ok(())
}

fn run_risk_order_capacity() -> Result<(), CapacityError> {
    let mut risk = RiskEngine::<1, 1>::new();
    risk.register_account(ACCOUNT, limits())
        .map_err(|_| CapacityError::at(CapacityCheck::RiskAccount))?;
    let first = order(1, 100, 1);
    let retry = order(2, 100, 2);
    risk.check_and_reserve(first)
        .map_err(|_| CapacityError::at(CapacityCheck::RiskSetup))?;
    if risk.check_and_reserve(retry) != Err(RejectReason::OrderCapacity) {
        return Err(CapacityError::at(CapacityCheck::RiskRefusal));
    }
    risk.settle(first.order_id, Quantity(0))
        .map_err(|_| CapacityError::at(CapacityCheck::RiskRelease))?;
    risk.check_and_reserve(retry)
        .map_err(|_| CapacityError::at(CapacityCheck::RiskRetry))?;
    Ok(())
}

fn run_account_capacity() -> Result<(), CapacityError> {
    let mut risk = RiskEngine::<1, 1>::new();
    risk.register_account(ACCOUNT, limits())
        .map_err(|_| CapacityError::at(CapacityCheck::AccountSetup))?;
    let first = order(1, 100, 1);
    risk.check_and_reserve(first)
        .map_err(|_| CapacityError::at(CapacityCheck::AccountSetup))?;
    let before = risk.export_state();
    let extra_account = AccountId(8);
    if risk.register_account(extra_account, limits()) != Err(RegistrationError::AccountCapacity) {
        return Err(CapacityError::at(CapacityCheck::AccountRefusal));
    }
    if risk.export_state() != before {
        return Err(CapacityError::at(CapacityCheck::AccountState));
    }
    risk.settle(first.order_id, Quantity(0))
        .map_err(|_| CapacityError::at(CapacityCheck::AccountRelease))?;
    let released = risk.export_state();
    // Releasing a reservation does not unregister its account.
    if risk.register_account(extra_account, limits()) != Err(RegistrationError::AccountCapacity)
        || risk.export_state() != released
    {
        return Err(CapacityError::at(
            CapacityCheck::AccountCapacityAfterRelease,
        ));
    }
    risk.check_and_reserve(order(2, 100, 2))
        .map_err(|_| CapacityError::at(CapacityCheck::AccountReuse))?;
    Ok(())
}

fn run_report_capacity() -> Result<(), CapacityError> {
    let mut risk = RiskEngine::<1, 4>::new();
    risk.register_account(ACCOUNT, limits())
        .map_err(|_| CapacityError::at(CapacityCheck::ReportSetup))?;
    let mut gateway = Gateway::<1, 4, 2, 2>::new(risk, INSTRUMENT);
    let mut reports = ReportBuffer::<1>::new();
    let first = NewOrder {
        side: Side::Sell,
        ..order(1, 100, 1)
    };
    let second = NewOrder {
        side: Side::Sell,
        ..order(2, 100, 2)
    };
    for maker in [first, second] {
        gateway
            .process_command(Command::NewOrder(maker), &mut reports)
            .map_err(|_| CapacityError::at(CapacityCheck::ReportSetup))?;
    }
    let before = gateway.export_state();
    let rejected = NewOrder {
        quantity: Quantity(2),
        ..order(3, 100, 3)
    };
    if gateway.process_command(Command::NewOrder(rejected), &mut reports)
        != Err(GatewayError::Book(RejectReason::ReportCapacity))
    {
        return Err(CapacityError::at(CapacityCheck::ReportRefusal));
    }
    let after = gateway.export_state();
    // Business rejection consumes the command sequence and order ID.
    if after.book != before.book
        || after.risk.accounts != before.risk.accounts
        || after.risk.reservations != before.risk.reservations
        || after.risk.killed != before.risk.killed
        || after.expected_sequence != SequenceNumber(4)
        || after.maximum_received_order_id != Some(rejected.order_id)
        || after.risk.maximum_order_id != Some(rejected.order_id)
        || !reports.is_empty()
    {
        return Err(CapacityError::at(CapacityCheck::ReportState));
    }
    gateway
        .process_command(Command::CancelOrder(cancel(second, 4)), &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::ReportCancel))?;
    let retry = NewOrder {
        quantity: rejected.quantity,
        ..order(4, 100, 5)
    };
    let outcome = gateway
        .process_command(Command::NewOrder(retry), &mut reports)
        .map_err(|_| CapacityError::at(CapacityCheck::ReportRetry))?;
    let report = reports.iter().next();
    if !matches!(
        outcome,
        GatewayOutcome::NewOrder(summary)
            if summary.filled_quantity == Quantity(1)
                && summary.resting_quantity == Quantity(1)
                && summary.discarded_quantity == Quantity(0)
                && summary.report_count == 1
    ) || !report.is_some_and(|report| {
        report.maker_order_id == first.order_id
            && report.taker_order_id == retry.order_id
            && report.quantity == Quantity(1)
    }) || gateway.expected_sequence() != SequenceNumber(6)
    {
        return Err(CapacityError::at(CapacityCheck::ReportRetry));
    }
    Ok(())
}

fn run_retransmit_capacity() -> Result<(), CapacityError> {
    let mut buffer = RetransmitBuffer::new(1);
    buffer
        .retain(SequenceNumber(1), b"first")
        .map_err(|_| CapacityError::at(CapacityCheck::RetransmitSetup))?;
    if buffer.retain(SequenceNumber(2), b"retry") != Err(RetainError::Full) {
        return Err(CapacityError::at(CapacityCheck::RetransmitRefusal));
    }
    if buffer.next_sequence() != 2 || buffer.confirm_through(1) != 1 {
        return Err(CapacityError::at(CapacityCheck::RetransmitRelease));
    }
    buffer
        .retain(SequenceNumber(2), b"retry")
        .map_err(|_| CapacityError::at(CapacityCheck::RetransmitRetry))?;
    Ok(())
}

fn run_command_queue_capacity() -> Result<(), CapacityError> {
    let mut commands = SpscQueue::<Command, 1>::try_new()
        .map_err(|_| CapacityError::at(CapacityCheck::CommandQueue))?;
    let (command_producer, mut command_consumer) = commands.split();
    let mut events = SpscQueue::<EventBatch<3>, 1>::try_new()
        .map_err(|_| CapacityError::at(CapacityCheck::CommandQueue))?;
    let (_event_producer, event_consumer) = events.split();
    let route = InstrumentRoute {
        instrument_id: INSTRUMENT,
        shard_id: ShardId(0),
    };
    let route_table =
        RouteTable::try_new([route]).map_err(|_| CapacityError::at(CapacityCheck::CommandRoute))?;
    let mut router =
        MultiInstrumentRouter::<1, 3, 1, 1>::new(route_table, [command_producer], [event_consumer]);
    let first = Command::NewOrder(order(1, 100, 1));
    let retry = Command::NewOrder(order(2, 100, 2));
    if router.route_command(first) != Ok(ShardId(0)) {
        return Err(CapacityError::at(CapacityCheck::CommandRoute));
    }
    if router.route_command(retry) != Err(RouterError::CommandBackpressured(ShardId(0))) {
        return Err(CapacityError::at(CapacityCheck::CommandRefusal));
    }
    if command_consumer.try_pop() != Some(first) {
        return Err(CapacityError::at(CapacityCheck::CommandRelease));
    }
    if router.route_command(retry) != Ok(ShardId(0)) || command_consumer.try_pop() != Some(retry) {
        return Err(CapacityError::at(CapacityCheck::CommandRetry));
    }
    Ok(())
}

fn run_event_queue_capacity() -> Result<(), CapacityError> {
    let mut risk = RiskEngine::<1, 2>::new();
    risk.register_account(ACCOUNT, limits())
        .map_err(|_| CapacityError::at(CapacityCheck::RiskAccount))?;
    let gateway = Gateway::<1, 2, 2, 2>::new(risk, INSTRUMENT);
    let mut events = SpscQueue::<EventBatch<3>, 1>::try_new()
        .map_err(|_| CapacityError::at(CapacityCheck::EventQueue))?;
    let (event_producer, mut event_consumer) = events.split();
    let mut engine = BoundedEventEngine::<1, 2, 2, 2, 1, 3, 1>::try_new(gateway, event_producer)
        .map_err(|_| CapacityError::at(CapacityCheck::EventEngine))?;
    let first = Command::NewOrder(order(1, 100, 1));
    let retry = Command::NewOrder(order(2, 100, 2));
    engine
        .process_command(first)
        .map_err(|_| CapacityError::at(CapacityCheck::EventProcess))?;
    if engine.process_command(retry) != Err(EventEngineError::Backpressured) {
        return Err(CapacityError::at(CapacityCheck::EventRefusal));
    }
    if engine.gateway().expected_sequence() != SequenceNumber(2) {
        return Err(CapacityError::at(CapacityCheck::EventState));
    }
    if event_consumer.try_pop().is_none() {
        return Err(CapacityError::at(CapacityCheck::EventRelease));
    }
    engine
        .process_command(retry)
        .map_err(|_| CapacityError::at(CapacityCheck::EventRetry))?;
    if engine.gateway().expected_sequence() != SequenceNumber(3)
        || event_consumer.try_pop().is_none()
    {
        return Err(CapacityError::at(CapacityCheck::EventRetry));
    }
    Ok(())
}

const fn order(id: u64, price: i64, sequence: u64) -> NewOrder {
    NewOrder {
        order_id: OrderId(id),
        account_id: ACCOUNT,
        instrument_id: INSTRUMENT,
        price: PriceTicks(price),
        quantity: Quantity(1),
        sequence: SequenceNumber(sequence),
        side: Side::Buy,
        time_in_force: TimeInForce::Gtc,
    }
}

const fn cancel(order: NewOrder, sequence: u64) -> CancelOrder {
    CancelOrder {
        order_id: order.order_id,
        account_id: order.account_id,
        instrument_id: order.instrument_id,
        sequence: SequenceNumber(sequence),
    }
}

const fn limits() -> RiskLimits {
    RiskLimits {
        max_quantity: Quantity(10),
        max_notional: 10_000,
        max_abs_position: Quantity(100),
        max_open_orders: 10,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_counts_are_exact_and_repeatable() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let expected = CapacityResult {
            price_level_order_refusals: 1,
            price_level_order_retries: 1,
            price_level_refusals: 1,
            price_level_retries: 1,
            risk_order_refusals: 1,
            risk_order_retries: 1,
            account_registration_refusals: 2,
            report_refusals: 1,
            report_retries: 1,
            retransmit_refusals: 1,
            retransmit_retries: 1,
            command_queue_refusals: 1,
            command_queue_retries: 1,
            event_queue_refusals: 1,
            event_queue_retries: 1,
        };
        assert_eq!(run_capacity(), Ok(expected));
        assert_eq!(run_capacity(), Ok(expected));
    }

    #[test]
    fn reservation_release_does_not_free_account_capacity() {
        assert_eq!(run_account_capacity(), Ok(()));
    }

    #[test]
    fn report_refusal_rolls_back_exposure_before_retry() {
        assert_eq!(run_report_capacity(), Ok(()));
    }
}
