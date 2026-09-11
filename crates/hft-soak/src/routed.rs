use crate::events::{EventSummary, EventValidationError, validate_batch};
use hft_book::TopLevel;
use hft_events::{Accepted, CommandKind, Event, EventBatch, EventEngineError};
use hft_gateway::{Gateway, GatewayError};
use hft_io::RxFrame;
use hft_model::Rng;
use hft_risk::{RiskEngine, RiskLimits};
use hft_router::{
    InstrumentRoute, MatchingShard, MultiInstrumentRouter, RouteTable, RouterError, ShardError,
    ShardId, ShardStep,
};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, OrderState, PriceTicks,
    Quantity, ReplaceOrder, SequenceNumber, Side, TimeInForce,
};
use hft_wire::{NEW_ORDER_TYPE, PROTOCOL_VERSION, ParseError};
use std::fmt;

const SHARDS: usize = 4;
const ACCOUNTS: usize = 4;
const RISK_ORDERS: usize = 32;
const LEVELS: usize = 8;
const ORDERS_PER_LEVEL: usize = 8;
const REPORTS: usize = 8;
const BATCH: usize = 10;
const COMMAND_QUEUE: usize = 8;
const EVENT_QUEUE: usize = 4;
const PRESSURE_INTERVAL: u64 = 256;
const PRESSURE_COMMANDS: usize = COMMAND_QUEUE + 1;
const LIVE_CHECK_INTERVAL: u64 = 1_024;
const LIVE_HIGH_WATER: usize = RISK_ORDERS * 3 / 4;
const SHARD_DIGEST_ROTATIONS: [u32; SHARDS] = [0, 1, 2, 3];

const ROUTES: [InstrumentRoute; SHARDS] = [
    InstrumentRoute {
        instrument_id: InstrumentId(11),
        shard_id: ShardId(0),
    },
    InstrumentRoute {
        instrument_id: InstrumentId(22),
        shard_id: ShardId(1),
    },
    InstrumentRoute {
        instrument_id: InstrumentId(33),
        shard_id: ShardId(2),
    },
    InstrumentRoute {
        instrument_id: InstrumentId(44),
        shard_id: ShardId(3),
    },
];

type SoakBatch = EventBatch<BATCH>;
type SoakGateway = Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>;
type SoakShard<'command, 'event> = MatchingShard<
    'command,
    'event,
    ACCOUNTS,
    RISK_ORDERS,
    LEVELS,
    ORDERS_PER_LEVEL,
    REPORTS,
    BATCH,
    COMMAND_QUEUE,
    EVENT_QUEUE,
>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoutedResult {
    pub steps: u64,
    pub routed_by_shard: [u64; SHARDS],
    pub events: u64,
    pub terminal_events: u64,
    pub accepted_events: u64,
    pub rejected_events: u64,
    pub cancelled_events: u64,
    pub replaced_events: u64,
    pub trade_events: u64,
    pub top_of_book_events: u64,
    pub command_backpressure: u64,
    pub event_backpressure: u64,
    pub pressure_rounds: u64,
    pub pending_retries: u64,
    pub last_pressure_step: u64,
    pub live_order_checks: u64,
    pub late_steps: u64,
    pub late_accepted_events: u64,
    pub late_rejected_events: u64,
    pub late_cancelled_events: u64,
    pub late_replaced_events: u64,
    pub late_trade_events: u64,
    pub sequence_gaps: u64,
    pub malformed_frames: u64,
    pub unknown_instruments: u64,
    pub state_fingerprint: u64,
    pub event_fingerprint: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedError {
    Setup,
    SequenceExhausted,
    UnexpectedRouter(RouterError),
    UnexpectedShard(ShardError),
    WrongShard,
    MissingEventBatch,
    ExtraEventBatch,
    StateChangedOnRejection,
    LiveOrders,
    Event(EventValidationError),
    EventContents(&'static str),
    AtStep {
        step: u64,
        command: Command,
        cause: Box<Self>,
    },
}

impl fmt::Display for RoutedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup => formatter.write_str("routed scenario setup failed"),
            Self::SequenceExhausted => formatter.write_str("routed sequence exhausted"),
            Self::UnexpectedRouter(error) => {
                write!(formatter, "unexpected router result: {error:?}")
            }
            Self::UnexpectedShard(error) => write!(formatter, "unexpected shard result: {error:?}"),
            Self::WrongShard => formatter.write_str("router selected the wrong shard"),
            Self::MissingEventBatch => formatter.write_str("processed command has no event batch"),
            Self::ExtraEventBatch => {
                formatter.write_str("rejected command published an event batch")
            }
            Self::StateChangedOnRejection => {
                formatter.write_str("rejected command changed shard state")
            }
            Self::LiveOrders => {
                formatter.write_str("live order table disagrees with events or state")
            }
            Self::Event(error) => write!(formatter, "invalid event batch: {error}"),
            Self::EventContents(name) => {
                write!(formatter, "event payload invariant failed: {name}")
            }
            Self::AtStep {
                step,
                command,
                cause,
            } => {
                write!(formatter, "step {step}, command {command:?}: {cause}")
            }
        }
    }
}

impl std::error::Error for RoutedError {}

impl RoutedError {
    fn at(self, record: RoutedCommand) -> Self {
        if matches!(&self, Self::AtStep { .. }) {
            self
        } else {
            Self::AtStep {
                step: record.step,
                command: record.command,
                cause: Box::new(self),
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LiveOrder {
    order_id: OrderId,
    account_id: AccountId,
    price: PriceTicks,
    quantity: Quantity,
    side: Side,
}

#[derive(Clone, Copy)]
struct LiveOrders {
    orders: [Option<LiveOrder>; RISK_ORDERS],
}

impl LiveOrders {
    const fn new() -> Self {
        Self {
            orders: [None; RISK_ORDERS],
        }
    }

    fn len(&self) -> usize {
        self.orders.iter().flatten().count()
    }

    fn pick(&self, rng: &mut Rng) -> Option<LiveOrder> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let mut rank = rng.below(u64::try_from(len).ok()?);
        for order in self.orders.iter().flatten() {
            if rank == 0 {
                return Some(*order);
            }
            rank -= 1;
        }
        None
    }

    fn slot(&mut self, order_id: OrderId) -> Result<&mut Option<LiveOrder>, RoutedError> {
        self.orders
            .iter_mut()
            .find(|slot| slot.is_some_and(|order| order.order_id == order_id))
            .ok_or(RoutedError::LiveOrders)
    }

    fn apply(&mut self, batch: &SoakBatch, command: Command) -> Result<EventSummary, RoutedError> {
        let summary = validate_batch(batch, command.instrument_id(), command.sequence())
            .map_err(RoutedError::Event)?;
        let before = (self.top(Side::Buy), self.top(Side::Sell));
        // Accepted quantities already exclude taker fills. Retire makers before inserting.
        let traded_quantity = self.apply_trades(batch, command)?;
        match (batch.iter().next(), command) {
            (Some(Event::Accepted(event)), Command::NewOrder(order))
                if event.order_id == order.order_id && event.account_id == order.account_id =>
            {
                validate_accepted(event, order, traded_quantity)?;
                if event.resting_quantity.0 != 0 {
                    if self
                        .orders
                        .iter()
                        .flatten()
                        .any(|live| live.order_id == order.order_id)
                    {
                        return Err(RoutedError::LiveOrders);
                    }
                    let slot = self
                        .orders
                        .iter_mut()
                        .find(|slot| slot.is_none())
                        .ok_or(RoutedError::LiveOrders)?;
                    *slot = Some(LiveOrder {
                        order_id: order.order_id,
                        account_id: order.account_id,
                        price: order.price,
                        quantity: event.resting_quantity,
                        side: order.side,
                    });
                }
            }
            (Some(Event::Cancelled(event)), Command::CancelOrder(cancel))
                if event.order_id == cancel.order_id && event.account_id == cancel.account_id =>
            {
                let slot = self.slot(event.order_id)?;
                let live = slot.as_ref().ok_or(RoutedError::LiveOrders)?;
                if live.account_id != event.account_id || live.quantity != event.quantity {
                    return Err(RoutedError::LiveOrders);
                }
                *slot = None;
            }
            (Some(Event::Replaced(event)), Command::ReplaceOrder(replace))
                if event.order_id == replace.order_id && event.account_id == replace.account_id =>
            {
                let live = self
                    .slot(event.order_id)?
                    .as_mut()
                    .ok_or(RoutedError::LiveOrders)?;
                if live.account_id != event.account_id
                    || live.quantity != event.old_quantity
                    || replace.quantity != event.new_quantity
                    || replace.price != event.price
                    || event.new_quantity.0 == 0
                    || event.priority_lost
                        == (replace.price == live.price && replace.quantity < live.quantity)
                {
                    return Err(RoutedError::LiveOrders);
                }
                live.quantity = event.new_quantity;
                live.price = event.price;
            }
            (Some(Event::Rejected(event)), command) => {
                let (kind, order_id, account_id) = match command {
                    Command::NewOrder(order) => {
                        (CommandKind::NewOrder, order.order_id, order.account_id)
                    }
                    Command::CancelOrder(cancel) => {
                        (CommandKind::Cancel, cancel.order_id, cancel.account_id)
                    }
                    Command::ReplaceOrder(replace) => {
                        (CommandKind::Replace, replace.order_id, replace.account_id)
                    }
                };
                if event.command != kind
                    || event.order_id != order_id
                    || event.account_id != account_id
                {
                    return Err(RoutedError::EventContents("rejected command identity"));
                }
            }
            _ => return Err(RoutedError::LiveOrders),
        }
        let after = (self.top(Side::Buy), self.top(Side::Sell));
        match batch.iter().last() {
            Some(Event::TopOfBook(event)) if before != after => {
                if (event.bid, event.ask) != after {
                    return Err(RoutedError::EventContents("top of book payload"));
                }
            }
            Some(Event::TopOfBook(_)) => {
                return Err(RoutedError::EventContents("unchanged top of book"));
            }
            _ if before != after => {
                return Err(RoutedError::EventContents("missing top of book"));
            }
            _ => {}
        }
        Ok(summary)
    }

    fn apply_trades(&mut self, batch: &SoakBatch, command: Command) -> Result<u64, RoutedError> {
        let mut traded_quantity = 0_u64;
        for event in batch.iter() {
            if let Event::Trade(trade) = event {
                let Command::NewOrder(taker) = command else {
                    return Err(RoutedError::EventContents("trade command kind"));
                };
                if trade.taker_order_id != taker.order_id || trade.quantity.0 == 0 {
                    return Err(RoutedError::EventContents("trade taker or quantity"));
                }
                let slot = self.slot(trade.maker_order_id)?;
                let order = slot.as_mut().ok_or(RoutedError::LiveOrders)?;
                let crosses = match taker.side {
                    Side::Buy => trade.price <= taker.price,
                    Side::Sell => trade.price >= taker.price,
                };
                if order.side == taker.side || trade.price != order.price || !crosses {
                    return Err(RoutedError::EventContents("trade maker or price"));
                }
                traded_quantity = traded_quantity
                    .checked_add(trade.quantity.0)
                    .ok_or(RoutedError::EventContents("trade quantity overflow"))?;
                order.quantity.0 = order
                    .quantity
                    .0
                    .checked_sub(trade.quantity.0)
                    .ok_or(RoutedError::LiveOrders)?;
                if order.quantity.0 == 0 {
                    *slot = None;
                }
            }
        }
        Ok(traded_quantity)
    }

    fn top(&self, side: Side) -> Option<TopLevel> {
        let mut top: Option<TopLevel> = None;
        for order in self
            .orders
            .iter()
            .flatten()
            .filter(|order| order.side == side)
        {
            match top.as_mut() {
                Some(level) if level.price == order.price => {
                    level.aggregate_quantity += u128::from(order.quantity.0);
                    level.order_count += 1;
                }
                Some(level)
                    if match side {
                        Side::Buy => level.price > order.price,
                        Side::Sell => level.price < order.price,
                    } => {}
                _ => {
                    top = Some(TopLevel {
                        price: order.price,
                        aggregate_quantity: u128::from(order.quantity.0),
                        order_count: 1,
                    });
                }
            }
        }
        top
    }

    fn validate(&self, gateway: &SoakGateway) -> Result<(), RoutedError> {
        let state = gateway.export_state();
        let mut count = 0;
        for (levels, len) in [
            (&state.book.bids, state.book.bid_level_count),
            (&state.book.asks, state.book.ask_level_count),
        ] {
            for level in levels.iter().take(len) {
                for order in level.orders.iter().take(level.order_count) {
                    count += 1;
                    if !self.orders.iter().flatten().any(|live| {
                        live.order_id == order.order_id
                            && live.account_id == order.account_id
                            && live.price == order.price
                            && live.quantity == order.quantity
                            && live.side == order.side
                    }) {
                        return Err(RoutedError::LiveOrders);
                    }
                }
            }
        }
        if count != self.len() {
            return Err(RoutedError::LiveOrders);
        }
        Ok(())
    }
}

fn validate_accepted(
    event: &Accepted,
    order: NewOrder,
    traded_quantity: u64,
) -> Result<(), RoutedError> {
    let total = event
        .filled_quantity
        .0
        .checked_add(event.resting_quantity.0)
        .and_then(|quantity| quantity.checked_add(event.discarded_quantity.0));
    if order.quantity.0 == 0
        || total != Some(order.quantity.0)
        || event.filled_quantity.0 != traded_quantity
    {
        return Err(RoutedError::EventContents("accepted quantity accounting"));
    }
    let expected_state = if event.filled_quantity == order.quantity {
        OrderState::Filled
    } else if event.filled_quantity.0 == 0 {
        OrderState::Accepted
    } else {
        OrderState::PartiallyFilled
    };
    if event.state != expected_state {
        return Err(RoutedError::EventContents("accepted order state"));
    }
    let valid_tif = match order.time_in_force {
        TimeInForce::Gtc => event.discarded_quantity.0 == 0,
        TimeInForce::Ioc => event.resting_quantity.0 == 0,
        TimeInForce::Fok => event.filled_quantity == order.quantity,
        TimeInForce::PostOnly => {
            event.filled_quantity.0 == 0 && event.resting_quantity == order.quantity
        }
    };
    if !valid_tif {
        return Err(RoutedError::EventContents("accepted time in force"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RoutedCommand {
    step: u64,
    shard_index: usize,
    command: Command,
}

struct ChurnState {
    rng: Rng,
    next_sequence: [u64; SHARDS],
    issued_orders: [u64; SHARDS],
    live: [LiveOrders; SHARDS],
}

impl ChurnState {
    fn command(&mut self, step: u64, shard_index: usize) -> Result<RoutedCommand, RoutedError> {
        let sequence = self.next_sequence[shard_index];
        let command = generated_command(
            &mut self.rng,
            shard_index,
            sequence,
            &mut self.issued_orders[shard_index],
            &self.live[shard_index],
        )?;
        let record = RoutedCommand {
            step,
            shard_index,
            command,
        };
        self.next_sequence[shard_index] = sequence
            .checked_add(1)
            .ok_or_else(|| RoutedError::SequenceExhausted.at(record))?;
        Ok(record)
    }
}

/// Runs routed traffic with pressure every 256 commands and live state checks every 1,024.
///
/// # Errors
///
/// Returns the first setup, routing, state, or event invariant failure.
pub fn run(seed: u64, steps: u64) -> Result<RoutedResult, RoutedError> {
    let mut result = RoutedResult {
        steps,
        late_steps: steps / 4,
        ..RoutedResult::default()
    };
    run_churn(seed, steps, &mut result)?;
    Ok(result)
}

fn run_churn(seed: u64, steps: u64, result: &mut RoutedResult) -> Result<(), RoutedError> {
    let mut command_zero =
        SpscQueue::<Command, COMMAND_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut command_one =
        SpscQueue::<Command, COMMAND_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut command_two =
        SpscQueue::<Command, COMMAND_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut command_three =
        SpscQueue::<Command, COMMAND_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let (command_zero_producer, command_zero_consumer) = command_zero.split();
    let (command_one_producer, command_one_consumer) = command_one.split();
    let (command_two_producer, command_two_consumer) = command_two.split();
    let (command_three_producer, command_three_consumer) = command_three.split();

    let mut event_zero =
        SpscQueue::<SoakBatch, EVENT_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut event_one =
        SpscQueue::<SoakBatch, EVENT_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut event_two =
        SpscQueue::<SoakBatch, EVENT_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let mut event_three =
        SpscQueue::<SoakBatch, EVENT_QUEUE>::try_new().map_err(|_| RoutedError::Setup)?;
    let (event_zero_producer, event_zero_consumer) = event_zero.split();
    let (event_one_producer, event_one_consumer) = event_one.split();
    let (event_two_producer, event_two_consumer) = event_two.split();
    let (event_three_producer, event_three_consumer) = event_three.split();

    let route_table = RouteTable::try_new([ROUTES[2], ROUTES[0], ROUTES[3], ROUTES[1]])
        .map_err(|_| RoutedError::Setup)?;
    let mut router = MultiInstrumentRouter::new(
        route_table,
        [
            command_zero_producer,
            command_one_producer,
            command_two_producer,
            command_three_producer,
        ],
        [
            event_zero_consumer,
            event_one_consumer,
            event_two_consumer,
            event_three_consumer,
        ],
    );
    let mut shards: Box<[SoakShard<'_, '_>; SHARDS]> = vec![
        MatchingShard::try_new(
            ROUTES[0],
            gateway(ROUTES[0].instrument_id)?,
            command_zero_consumer,
            event_zero_producer,
        )
        .map_err(|_| RoutedError::Setup)?,
        MatchingShard::try_new(
            ROUTES[1],
            gateway(ROUTES[1].instrument_id)?,
            command_one_consumer,
            event_one_producer,
        )
        .map_err(|_| RoutedError::Setup)?,
        MatchingShard::try_new(
            ROUTES[2],
            gateway(ROUTES[2].instrument_id)?,
            command_two_consumer,
            event_two_producer,
        )
        .map_err(|_| RoutedError::Setup)?,
        MatchingShard::try_new(
            ROUTES[3],
            gateway(ROUTES[3].instrument_id)?,
            command_three_consumer,
            event_three_producer,
        )
        .map_err(|_| RoutedError::Setup)?,
    ]
    .into_boxed_slice()
    .try_into()
    .map_err(|_| RoutedError::Setup)?;

    let mut state = ChurnState {
        rng: Rng::new(seed),
        next_sequence: [1; SHARDS],
        issued_orders: [0; SHARDS],
        live: [LiveOrders::new(); SHARDS],
    };
    execute_churn(steps, &mut state, &mut router, shards.as_mut(), result)?;

    result.state_fingerprint = shards
        .iter()
        .enumerate()
        .fold(0_u64, |hash, (index, shard)| {
            mix(
                hash,
                shard
                    .gateway()
                    .stable_digest()
                    .rotate_left(SHARD_DIGEST_ROTATIONS[index]),
            )
        });
    Ok(())
}

fn execute_churn(
    steps: u64,
    state: &mut ChurnState,
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &mut [SoakShard<'_, '_>],
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let pressure_steps = u64::try_from(PRESSURE_COMMANDS).map_err(|_| RoutedError::Setup)?;
    let mut step = 0;
    while step < steps {
        let shard_index = weighted_shard(&mut state.rng);
        let record = state.command(step, shard_index)?;
        if step % LIVE_CHECK_INTERVAL == 0 {
            validate_live(state, shards, result).map_err(|error| error.at(record))?;
        }
        if step % 113 == 0 {
            reject_malformed_frame(router, shards, result).map_err(|error| error.at(record))?;
        }
        if step % 127 == 0 {
            reject_unknown_instrument(router, shards, result).map_err(|error| error.at(record))?;
        }
        if step % 97 == 0 {
            reject_sequence_gap(router, shards, shard_index, record.command, result)
                .map_err(|error| error.at(record))?;
        }
        if step % PRESSURE_INTERVAL == 0 && steps - step >= pressure_steps {
            let mut records = [record; PRESSURE_COMMANDS];
            for (offset, slot) in records.iter_mut().enumerate().skip(1) {
                let offset = u64::try_from(offset).map_err(|_| RoutedError::Setup)?;
                *slot = state.command(step + offset, shard_index)?;
            }
            run_pressure(
                &records,
                router,
                shards,
                &mut state.live[shard_index],
                result,
            )
            .map_err(|error| error.at(record))?;
            step += pressure_steps;
        } else {
            process_command(router, shards, record, &mut state.live[shard_index], result)
                .map_err(|error| error.at(record))?;
            step += 1;
        }
        if step == steps {
            validate_live(state, shards, result).map_err(|error| error.at(record))?;
        }
    }

    Ok(())
}

fn validate_live(
    state: &ChurnState,
    shards: &[SoakShard<'_, '_>],
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    for (live, shard) in state.live.iter().zip(shards) {
        live.validate(shard.gateway())?;
        result.live_order_checks = result.live_order_checks.saturating_add(1);
    }
    Ok(())
}

fn run_pressure(
    records: &[RoutedCommand; PRESSURE_COMMANDS],
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &mut [SoakShard<'_, '_>],
    live: &mut LiveOrders,
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let shard_index = records[0].shard_index;
    let route = ROUTES[shard_index];
    let before = shard_digests(shards);
    for record in &records[..COMMAND_QUEUE] {
        route_expected(router, record.command, route.shard_id)
            .map_err(|error| error.at(*record))?;
    }
    let retry = records[COMMAND_QUEUE];
    if router.route_command(retry.command) != Err(RouterError::CommandBackpressured(route.shard_id))
    {
        return Err(
            RoutedError::UnexpectedRouter(RouterError::CommandBackpressured(route.shard_id))
                .at(retry),
        );
    }
    if shard_digests(shards) != before {
        return Err(RoutedError::StateChangedOnRejection.at(retry));
    }
    result.command_backpressure = result.command_backpressure.saturating_add(1);
    for record in &records[..EVENT_QUEUE] {
        expect_processed(&mut shards[shard_index], record.command.sequence())
            .map_err(|error| error.at(*record))?;
    }
    let blocked = records[EVENT_QUEUE];
    let blocked_state = shard_digests(shards);
    for _ in 0..2 {
        if shards[shard_index].try_process_one() != Err(ShardError::EventBackpressured) {
            return Err(RoutedError::UnexpectedShard(ShardError::EventBackpressured).at(blocked));
        }
        if !shards[shard_index].has_pending_command()
            || shards[shard_index].gateway().expected_sequence() != blocked.command.sequence()
            || shard_digests(shards) != blocked_state
        {
            return Err(RoutedError::StateChangedOnRejection.at(blocked));
        }
        result.event_backpressure = result.event_backpressure.saturating_add(1);
    }
    for record in &records[..EVENT_QUEUE] {
        drain_and_validate(router, *record, live, result).map_err(|error| error.at(*record))?;
    }
    route_expected(router, retry.command, route.shard_id).map_err(|error| error.at(retry))?;
    for record in &records[EVENT_QUEUE..] {
        expect_processed(&mut shards[shard_index], record.command.sequence())
            .map_err(|error| error.at(*record))?;
        drain_and_validate(router, *record, live, result).map_err(|error| error.at(*record))?;
    }
    if shards[shard_index].has_pending_command() {
        return Err(RoutedError::StateChangedOnRejection.at(blocked));
    }
    expect_empty(router, route.shard_id)?;
    validate_other_shards(shards, shard_index, before)?;
    result.pressure_rounds = result.pressure_rounds.saturating_add(1);
    result.pending_retries = result.pending_retries.saturating_add(1);
    result.last_pressure_step = records[0].step;
    Ok(())
}

fn process_command(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &mut [SoakShard<'_, '_>],
    record: RoutedCommand,
    live: &mut LiveOrders,
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let shard_id = ROUTES[record.shard_index].shard_id;
    route_expected(router, record.command, shard_id)?;
    let before = shard_digests(shards);
    expect_processed(&mut shards[record.shard_index], record.command.sequence())?;
    validate_other_shards(shards, record.shard_index, before)?;
    drain_and_validate(router, record, live, result)?;
    expect_empty(router, shard_id)
}

fn validate_other_shards(
    shards: &[SoakShard<'_, '_>],
    shard_index: usize,
    before: [u64; SHARDS],
) -> Result<(), RoutedError> {
    for (index, shard) in shards.iter().enumerate() {
        if index != shard_index && shard.gateway().stable_digest() != before[index] {
            return Err(RoutedError::StateChangedOnRejection);
        }
    }
    Ok(())
}

fn reject_sequence_gap(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &mut [SoakShard<'_, '_>],
    shard_index: usize,
    command: Command,
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let before = shard_digests(shards);
    let received = command
        .sequence()
        .0
        .checked_add(1)
        .ok_or(RoutedError::SequenceExhausted)?;
    let gap = with_sequence(command, SequenceNumber(received));
    let shard_id = ROUTES[shard_index].shard_id;
    route_expected(router, gap, shard_id)?;
    let expected = command.sequence();
    let wanted = ShardError::Engine(EventEngineError::Gateway(GatewayError::Sequence {
        expected,
        received: SequenceNumber(received),
    }));
    if shards[shard_index].try_process_one() != Err(wanted) {
        return Err(RoutedError::UnexpectedShard(wanted));
    }
    if shard_digests(shards) != before {
        return Err(RoutedError::StateChangedOnRejection);
    }
    if router
        .try_event(shard_id)
        .map_err(RoutedError::UnexpectedRouter)?
        .is_some()
    {
        return Err(RoutedError::ExtraEventBatch);
    }
    result.sequence_gaps = result.sequence_gaps.saturating_add(1);
    Ok(())
}

fn reject_malformed_frame(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &[SoakShard<'_, '_>],
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let before = shard_digests(shards);
    let malformed = [PROTOCOL_VERSION, NEW_ORDER_TYPE, 0];
    let response = router.route_frame(&RxFrame::from_bytes(&malformed));
    if response != Err(RouterError::Parse(ParseError::TruncatedHeader)) {
        return Err(response
            .err()
            .map_or(RoutedError::WrongShard, RoutedError::UnexpectedRouter));
    }
    if shard_digests(shards) != before {
        return Err(RoutedError::StateChangedOnRejection);
    }
    result.malformed_frames = result.malformed_frames.saturating_add(1);
    Ok(())
}

fn reject_unknown_instrument(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shards: &[SoakShard<'_, '_>],
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let before = shard_digests(shards);
    let command = Command::CancelOrder(CancelOrder {
        order_id: OrderId(1),
        account_id: AccountId(1),
        instrument_id: InstrumentId(999),
        sequence: SequenceNumber(1),
    });
    let response = router.route_command(command);
    if response != Err(RouterError::UnknownInstrument(InstrumentId(999))) {
        return Err(response
            .err()
            .map_or(RoutedError::WrongShard, RoutedError::UnexpectedRouter));
    }
    if shard_digests(shards) != before {
        return Err(RoutedError::StateChangedOnRejection);
    }
    result.unknown_instruments = result.unknown_instruments.saturating_add(1);
    Ok(())
}

fn route_expected<const COUNT: usize, const COMMANDS: usize, const EVENTS: usize>(
    router: &mut MultiInstrumentRouter<'_, '_, COUNT, BATCH, COMMANDS, EVENTS>,
    command: Command,
    expected: ShardId,
) -> Result<(), RoutedError> {
    let actual = router
        .route_command(command)
        .map_err(RoutedError::UnexpectedRouter)?;
    if actual != expected {
        return Err(RoutedError::WrongShard);
    }
    Ok(())
}

fn expect_processed<const COMMANDS: usize, const EVENTS: usize>(
    shard: &mut MatchingShard<
        '_,
        '_,
        ACCOUNTS,
        RISK_ORDERS,
        LEVELS,
        ORDERS_PER_LEVEL,
        REPORTS,
        BATCH,
        COMMANDS,
        EVENTS,
    >,
    sequence: SequenceNumber,
) -> Result<(), RoutedError> {
    match shard.try_process_one() {
        Ok(ShardStep::Processed(actual)) if actual == sequence => Ok(()),
        Ok(_) => Err(RoutedError::WrongShard),
        Err(error) => Err(RoutedError::UnexpectedShard(error)),
    }
}

fn drain_and_validate(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    record: RoutedCommand,
    live: &mut LiveOrders,
    result: &mut RoutedResult,
) -> Result<(), RoutedError> {
    let route = ROUTES[record.shard_index];
    let batch = router
        .try_event(route.shard_id)
        .map_err(RoutedError::UnexpectedRouter)?
        .ok_or(RoutedError::MissingEventBatch)?;
    let summary = live.apply(&batch, record.command)?;
    absorb_events(result, summary, record.step);
    result.routed_by_shard[record.shard_index] =
        result.routed_by_shard[record.shard_index].saturating_add(1);
    Ok(())
}

fn expect_empty(
    router: &mut MultiInstrumentRouter<'_, '_, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>,
    shard_id: ShardId,
) -> Result<(), RoutedError> {
    if router
        .try_event(shard_id)
        .map_err(RoutedError::UnexpectedRouter)?
        .is_some()
    {
        return Err(RoutedError::ExtraEventBatch);
    }
    Ok(())
}

fn absorb_events(result: &mut RoutedResult, summary: EventSummary, step: u64) {
    result.events = result.events.saturating_add(summary.events);
    result.terminal_events = result.terminal_events.saturating_add(summary.terminals);
    result.accepted_events = result.accepted_events.saturating_add(summary.accepted);
    result.rejected_events = result.rejected_events.saturating_add(summary.rejected);
    result.cancelled_events = result.cancelled_events.saturating_add(summary.cancelled);
    result.replaced_events = result.replaced_events.saturating_add(summary.replaced);
    result.trade_events = result.trade_events.saturating_add(summary.trades);
    result.top_of_book_events = result
        .top_of_book_events
        .saturating_add(summary.top_of_book);
    result.event_fingerprint = mix(result.event_fingerprint, summary.fingerprint);
    if step >= result.steps - result.late_steps {
        result.late_accepted_events = result.late_accepted_events.saturating_add(summary.accepted);
        result.late_rejected_events = result.late_rejected_events.saturating_add(summary.rejected);
        result.late_cancelled_events = result
            .late_cancelled_events
            .saturating_add(summary.cancelled);
        result.late_replaced_events = result.late_replaced_events.saturating_add(summary.replaced);
        result.late_trade_events = result.late_trade_events.saturating_add(summary.trades);
    }
}

fn gateway(instrument: InstrumentId) -> Result<SoakGateway, RoutedError> {
    let limits = RiskLimits {
        max_quantity: Quantity(16),
        max_notional: 1_000_000,
        max_abs_position: Quantity(u64::MAX),
        max_open_orders: 32,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    };
    let mut risk = RiskEngine::new();
    for account in 1_u32..=4 {
        risk.register_account(AccountId(account), limits)
            .map_err(|_| RoutedError::Setup)?;
    }
    Ok(Gateway::new(risk, instrument))
}

fn generated_command(
    rng: &mut Rng,
    shard_index: usize,
    sequence: u64,
    issued: &mut u64,
    live: &LiveOrders,
) -> Result<Command, RoutedError> {
    let instrument_id = ROUTES[shard_index].instrument_id;
    let account_id = AccountId(match rng.below(4) {
        0 => 1,
        1 => 2,
        2 => 3,
        _ => 4,
    });
    let draw = rng.below(100);
    let target = live.pick(rng);
    if target.is_none() || (draw < 65 && live.len() < LIVE_HIGH_WATER) {
        *issued = issued
            .checked_add(1)
            .ok_or(RoutedError::SequenceExhausted)?;
        let order_id = OrderId(
            u64::from(instrument_id.0)
                .checked_mul(1_000_000_000)
                .and_then(|base| base.checked_add(*issued))
                .ok_or(RoutedError::SequenceExhausted)?,
        );
        Ok(Command::NewOrder(NewOrder {
            order_id,
            account_id,
            instrument_id,
            price: PriceTicks(random_price(rng)),
            quantity: Quantity(1 + rng.below(8)),
            sequence: SequenceNumber(sequence),
            side: if rng.coin() { Side::Buy } else { Side::Sell },
            time_in_force: match rng.below(10) {
                0 => TimeInForce::Ioc,
                1 => TimeInForce::Fok,
                2 => TimeInForce::PostOnly,
                _ => TimeInForce::Gtc,
            },
        }))
    } else {
        let target = target.ok_or(RoutedError::LiveOrders)?;
        if draw < 83 || live.len() >= LIVE_HIGH_WATER {
            Ok(Command::CancelOrder(CancelOrder {
                order_id: target.order_id,
                account_id: target.account_id,
                instrument_id,
                sequence: SequenceNumber(sequence),
            }))
        } else {
            Ok(Command::ReplaceOrder(ReplaceOrder {
                order_id: target.order_id,
                account_id: target.account_id,
                instrument_id,
                sequence: SequenceNumber(sequence),
                price: if rng.coin() {
                    target.price
                } else {
                    PriceTicks(random_price(rng))
                },
                quantity: Quantity(1 + rng.below(16)),
            }))
        }
    }
}

fn weighted_shard(rng: &mut Rng) -> usize {
    let draw = rng.below(100);
    if draw < 85 {
        0
    } else if draw < 95 {
        1
    } else if draw < 99 {
        2
    } else {
        3
    }
}

fn random_price(rng: &mut Rng) -> i64 {
    match rng.below(5) {
        0 => 98,
        1 => 99,
        2 => 100,
        3 => 101,
        _ => 102,
    }
}

const fn with_sequence(command: Command, sequence: SequenceNumber) -> Command {
    match command {
        Command::NewOrder(mut value) => {
            value.sequence = sequence;
            Command::NewOrder(value)
        }
        Command::CancelOrder(mut value) => {
            value.sequence = sequence;
            Command::CancelOrder(value)
        }
        Command::ReplaceOrder(mut value) => {
            value.sequence = sequence;
            Command::ReplaceOrder(value)
        }
    }
}

fn shard_digests(shards: &[SoakShard<'_, '_>]) -> [u64; SHARDS] {
    std::array::from_fn(|index| shards[index].gateway().stable_digest())
}

const fn mix(hash: u64, value: u64) -> u64 {
    hash.rotate_left(13)
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routed_run_repeats_exactly() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let first = run(0x5eed_1234, 2_000).expect("first run");
        let second = run(0x5eed_1234, 2_000).expect("second run");
        assert_eq!(first, second);
        assert_eq!(first.routed_by_shard.iter().sum::<u64>(), 2_000);
        assert_eq!(first.terminal_events, 2_000);
        assert!(first.routed_by_shard.iter().all(|count| *count > 0));
        assert!(first.command_backpressure > 0);
        assert!(first.event_backpressure > 0);
        assert!(first.sequence_gaps > 0);
        assert!(first.malformed_frames > 0);
        assert!(first.unknown_instruments > 0);
        assert_eq!(
            first.terminal_events,
            first
                .events
                .saturating_sub(first.trade_events)
                .saturating_sub(first.top_of_book_events)
        );
    }

    #[test]
    fn late_churn_keeps_trades_cancels_and_replaces_with_recurring_pressure() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        for seed in [0, 0x5eed_1234, u64::MAX] {
            let result = run(seed, 20_000).expect("sustained routed churn");
            assert_eq!(result.routed_by_shard.iter().sum::<u64>(), result.steps);
            assert!(result.routed_by_shard[0] > result.steps * 4 / 5);
            assert!(result.routed_by_shard.iter().all(|count| *count > 0));
            assert_eq!(result.terminal_events, result.steps);
            assert_eq!(
                result.accepted_events
                    + result.rejected_events
                    + result.cancelled_events
                    + result.replaced_events,
                result.steps
            );
            assert_eq!(result.late_steps, 5_000);
            assert_eq!(
                result.late_accepted_events
                    + result.late_rejected_events
                    + result.late_cancelled_events
                    + result.late_replaced_events,
                result.late_steps
            );
            assert!(result.late_accepted_events > 1_500, "{result:?}");
            assert!(result.late_trade_events > 500, "{result:?}");
            assert!(result.late_cancelled_events > 500, "{result:?}");
            assert!(result.late_replaced_events > 250, "{result:?}");
            assert!(result.late_rejected_events < 1_500, "{result:?}");
            assert_eq!(result.pressure_rounds, 79);
            assert_eq!(result.command_backpressure, result.pressure_rounds);
            assert_eq!(result.event_backpressure, result.pressure_rounds * 2);
            assert_eq!(result.pending_retries, result.pressure_rounds);
            assert_eq!(result.last_pressure_step, 19_968);
            assert_eq!(result.live_order_checks, 84);
        }
    }

    fn partial_fill() -> (LiveOrders, NewOrder, [Event; 3]) {
        use hft_events::{EventId, TopOfBook, Trade};
        let mut live = LiveOrders::new();
        live.orders[0] = Some(LiveOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            price: PriceTicks(100),
            quantity: Quantity(2),
            side: Side::Sell,
        });
        let taker = NewOrder {
            order_id: OrderId(2),
            account_id: AccountId(2),
            instrument_id: ROUTES[0].instrument_id,
            price: PriceTicks(100),
            quantity: Quantity(5),
            sequence: SequenceNumber(2),
            side: Side::Buy,
            time_in_force: TimeInForce::Gtc,
        };
        let events = [
            Event::Accepted(Accepted {
                id: EventId {
                    command_sequence: taker.sequence,
                    ordinal: 0,
                },
                order_id: taker.order_id,
                account_id: taker.account_id,
                instrument_id: taker.instrument_id,
                state: OrderState::PartiallyFilled,
                filled_quantity: Quantity(2),
                resting_quantity: Quantity(3),
                discarded_quantity: Quantity(0),
            }),
            Event::Trade(Trade {
                id: EventId {
                    command_sequence: taker.sequence,
                    ordinal: 1,
                },
                maker_order_id: OrderId(1),
                taker_order_id: taker.order_id,
                instrument_id: taker.instrument_id,
                price: PriceTicks(100),
                quantity: Quantity(2),
            }),
            Event::TopOfBook(TopOfBook {
                id: EventId {
                    command_sequence: taker.sequence,
                    ordinal: 2,
                },
                instrument_id: taker.instrument_id,
                bid: Some(TopLevel {
                    price: taker.price,
                    aggregate_quantity: 3,
                    order_count: 1,
                }),
                ask: None,
            }),
        ];
        (live, taker, events)
    }

    fn batch(events: &[Event]) -> SoakBatch {
        let mut batch = SoakBatch::new();
        for event in events {
            batch.try_push(*event).expect("batch capacity");
        }
        batch
    }

    fn apply_batch(
        mut live: LiveOrders,
        events: &[Event],
        command: Command,
    ) -> Result<EventSummary, RoutedError> {
        live.apply(&batch(events), command)
    }

    #[test]
    fn partial_taker_resting_quantity_is_not_decremented_twice() {
        let (mut live, taker, events) = partial_fill();
        let batch = batch(&events);
        live.apply(&batch, Command::NewOrder(taker))
            .expect("live update");
        assert_eq!(live.len(), 1);
        let remaining = live.pick(&mut Rng::new(1)).expect("resting taker");
        assert_eq!(remaining.order_id, taker.order_id);
        assert_eq!(remaining.quantity, Quantity(3));
    }

    #[test]
    fn rejects_wrong_rejection_identity() {
        use hft_events::{EventId, Rejected};
        use hft_types::RejectReason;

        let command = CancelOrder {
            order_id: OrderId(2),
            account_id: AccountId(1),
            instrument_id: ROUTES[0].instrument_id,
            sequence: SequenceNumber(3),
        };
        let rejected = Rejected {
            id: EventId {
                command_sequence: command.sequence,
                ordinal: 0,
            },
            command: CommandKind::Cancel,
            order_id: command.order_id,
            account_id: command.account_id,
            instrument_id: command.instrument_id,
            reason: RejectReason::UnknownOrder,
        };
        LiveOrders::new()
            .apply(
                &batch(&[Event::Rejected(rejected)]),
                Command::CancelOrder(command),
            )
            .expect("matching rejection identity");
        for wrong in [
            Rejected {
                order_id: OrderId(99),
                ..rejected
            },
            Rejected {
                account_id: AccountId(99),
                ..rejected
            },
            Rejected {
                command: CommandKind::Replace,
                ..rejected
            },
        ] {
            assert_eq!(
                LiveOrders::new().apply(
                    &batch(&[Event::Rejected(wrong)]),
                    Command::CancelOrder(command),
                ),
                Err(RoutedError::EventContents("rejected command identity")),
            );
        }
    }

    #[test]
    fn rejects_wrong_trade_taker_price_and_quantity() {
        use hft_events::Trade;

        let (live, taker, events) = partial_fill();
        let Event::Trade(trade) = events[1] else {
            panic!("trade fixture");
        };
        for wrong in [
            Trade {
                taker_order_id: OrderId(99),
                ..trade
            },
            Trade {
                price: PriceTicks(99),
                ..trade
            },
            Trade {
                quantity: Quantity(0),
                ..trade
            },
            Trade {
                quantity: Quantity(3),
                ..trade
            },
        ] {
            let mut wrong_events = events;
            wrong_events[1] = Event::Trade(wrong);
            assert!(apply_batch(live, &wrong_events, Command::NewOrder(taker)).is_err());
        }
        let mut same_side = live;
        same_side.orders[0].as_mut().expect("maker").side = taker.side;
        assert_eq!(
            same_side.apply(&batch(&events), Command::NewOrder(taker)),
            Err(RoutedError::EventContents("trade maker or price")),
        );
        let mut noncrossing = taker;
        noncrossing.price = PriceTicks(99);
        assert_eq!(
            apply_batch(live, &events, Command::NewOrder(noncrossing)),
            Err(RoutedError::EventContents("trade maker or price")),
        );
    }

    #[test]
    fn rejects_wrong_accepted_accounting_state_and_time_in_force() {
        let (live, taker, events) = partial_fill();
        let Event::Accepted(accepted) = events[0] else {
            panic!("accepted fixture");
        };
        for wrong in [
            Accepted {
                resting_quantity: Quantity(4),
                ..accepted
            },
            Accepted {
                filled_quantity: Quantity(1),
                resting_quantity: Quantity(4),
                ..accepted
            },
            Accepted {
                filled_quantity: Quantity(u64::MAX),
                ..accepted
            },
            Accepted {
                state: OrderState::Filled,
                ..accepted
            },
            Accepted {
                resting_quantity: Quantity(2),
                discarded_quantity: Quantity(1),
                ..accepted
            },
        ] {
            let mut wrong_events = events;
            wrong_events[0] = Event::Accepted(wrong);
            assert!(apply_batch(live, &wrong_events, Command::NewOrder(taker)).is_err());
        }
        for time_in_force in [TimeInForce::Ioc, TimeInForce::Fok, TimeInForce::PostOnly] {
            assert_eq!(
                apply_batch(
                    live,
                    &events,
                    Command::NewOrder(NewOrder {
                        time_in_force,
                        ..taker
                    }),
                ),
                Err(RoutedError::EventContents("accepted time in force")),
            );
        }
    }

    #[test]
    fn rejects_wrong_or_missing_top_of_book() {
        let (live, taker, events) = partial_fill();
        let Event::TopOfBook(top) = events[2] else {
            panic!("top fixture");
        };
        let bid = top.bid.expect("bid");
        for wrong_bid in [
            None,
            Some(TopLevel {
                price: PriceTicks(99),
                ..bid
            }),
            Some(TopLevel {
                aggregate_quantity: 4,
                ..bid
            }),
            Some(TopLevel {
                order_count: 2,
                ..bid
            }),
        ] {
            let mut wrong_events = events;
            wrong_events[2] = Event::TopOfBook(hft_events::TopOfBook {
                bid: wrong_bid,
                ..top
            });
            assert_eq!(
                apply_batch(live, &wrong_events, Command::NewOrder(taker)),
                Err(RoutedError::EventContents("top of book payload")),
            );
        }
        assert_eq!(
            apply_batch(live, &events[..2], Command::NewOrder(taker)),
            Err(RoutedError::EventContents("missing top of book")),
        );
    }
}
