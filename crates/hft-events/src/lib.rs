#![forbid(unsafe_code)]

use hft_book::TopLevel;
use hft_gateway::{Gateway, GatewayError, GatewayOutcome};
use hft_io::RxFrame;
use hft_spsc::Producer;
use hft_types::{
    AccountId, Command, InstrumentId, OrderId, OrderState, PriceTicks, Quantity, RejectReason,
    ReportBuffer, SequenceNumber, Side,
};
use hft_wire::{BorrowedMessage, parse_message};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventId {
    pub command_sequence: SequenceNumber,
    pub ordinal: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandKind {
    NewOrder,
    Cancel,
    Replace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Accepted {
    pub id: EventId,
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub state: OrderState,
    pub filled_quantity: Quantity,
    pub resting_quantity: Quantity,
    pub discarded_quantity: Quantity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rejected {
    pub id: EventId,
    pub command: CommandKind,
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub reason: RejectReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trade {
    pub id: EventId,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub instrument_id: InstrumentId,
    pub price: PriceTicks,
    pub quantity: Quantity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cancelled {
    pub id: EventId,
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub quantity: Quantity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Replaced {
    pub id: EventId,
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub old_quantity: Quantity,
    pub new_quantity: Quantity,
    pub price: PriceTicks,
    pub priority_lost: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopOfBook {
    pub id: EventId,
    pub instrument_id: InstrumentId,
    pub bid: Option<TopLevel>,
    pub ask: Option<TopLevel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    Accepted(Accepted),
    Rejected(Rejected),
    Trade(Trade),
    Cancelled(Cancelled),
    Replaced(Replaced),
    TopOfBook(TopOfBook),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventBatch<const N: usize> {
    events: [Option<Event>; N],
    len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchFull;

impl<const N: usize> EventBatch<N> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            events: [None; N],
            len: 0,
        }
    }

    /// # Errors
    ///
    /// Returns [`BatchFull`] when the fixed-size batch has no free slot.
    pub fn try_push(&mut self, event: Event) -> Result<(), BatchFull> {
        let Some(slot) = self.events.get_mut(self.len) else {
            return Err(BatchFull);
        };
        *slot = Some(event);
        self.len += 1;
        Ok(())
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &Event> {
        self.events[..self.len].iter().filter_map(Option::as_ref)
    }
}

impl<const N: usize> Default for EventBatch<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventEngineConfigError {
    BatchTooSmall,
    TooManyEvents,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventEngineError {
    Backpressured,
    Gateway(GatewayError),
    EventCapacityInvariant,
    PublicationInvariant,
}

pub struct BoundedEventEngine<
    'queue,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const QUEUE: usize,
> {
    gateway: Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>,
    producer: Producer<'queue, EventBatch<BATCH>, QUEUE>,
}

/// One command with checked sequence and event capacity.
///
/// The token holds the exclusive engine borrow. Dropping it leaves gateway
/// state and the event queue unchanged.
#[must_use]
pub struct AdmittedCommand<
    'engine,
    'queue,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const QUEUE: usize,
> {
    engine: &'engine mut BoundedEventEngine<
        'queue,
        ACCOUNTS,
        RISK_ORDERS,
        LEVELS,
        ORDERS_PER_LEVEL,
        REPORTS,
        BATCH,
        QUEUE,
    >,
    command: Command,
}

impl<
    'queue,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const QUEUE: usize,
>
    BoundedEventEngine<
        'queue,
        ACCOUNTS,
        RISK_ORDERS,
        LEVELS,
        ORDERS_PER_LEVEL,
        REPORTS,
        BATCH,
        QUEUE,
    >
{
    /// Builds an engine when one batch can hold the largest command result.
    ///
    /// # Errors
    ///
    /// Returns [`EventEngineConfigError::BatchTooSmall`] when the batch cannot
    /// hold every report, one terminal event, and one top-of-book event. Returns
    /// [`EventEngineConfigError::TooManyEvents`] when ordinals cannot represent
    /// the configured report count.
    pub fn try_new(
        gateway: Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>,
        producer: Producer<'queue, EventBatch<BATCH>, QUEUE>,
    ) -> Result<Self, EventEngineConfigError> {
        if REPORTS > usize::from(u16::MAX) - 1 {
            return Err(EventEngineConfigError::TooManyEvents);
        }
        if BATCH < REPORTS.saturating_add(2) {
            return Err(EventEngineConfigError::BatchTooSmall);
        }
        Ok(Self { gateway, producer })
    }

    /// Processes one sequence-valid command and publishes its event batch.
    ///
    /// Queue capacity is reserved before the gateway advances its sequence or
    /// mutates state. Parse and sequence failures publish no batch.
    ///
    /// # Errors
    ///
    /// Returns [`EventEngineError::Backpressured`] without processing when the
    /// queue is full. Parse, sequence, and fatal gateway errors are returned as
    /// [`EventEngineError::Gateway`]. Sequence-valid business rejections are
    /// published as [`Event::Rejected`] and return success. An invariant error
    /// indicates a configuration or producer contract defect after mutation.
    pub fn process_frame(&mut self, frame: &RxFrame<'_>) -> Result<(), EventEngineError> {
        let message = parse_message(frame)
            .map_err(|error| EventEngineError::Gateway(GatewayError::Parse(error)))?;
        let sequence = message.sequence();
        self.preflight(sequence)?;
        let command = CommandIdentity::from_message(message);
        let before = (
            self.gateway.top_level(Side::Buy),
            self.gateway.top_level(Side::Sell),
        );
        let mut reports = ReportBuffer::<REPORTS>::new();
        let result = self.gateway.process_frame(frame, &mut reports);
        let after = (
            self.gateway.top_level(Side::Buy),
            self.gateway.top_level(Side::Sell),
        );
        self.publish_result(command, sequence, before, after, &reports, result)
    }

    /// Processes one normalized command from a bounded shard queue.
    ///
    /// # Errors
    ///
    /// Uses the same sequence, capacity, gateway, and publication errors as
    /// [`Self::process_frame`].
    pub fn process_command(&mut self, command: Command) -> Result<(), EventEngineError> {
        self.admit(command)?.apply()
    }

    /// Checks sequence and event capacity without applying the command.
    ///
    /// The returned token keeps one event slot available until application.
    /// Dropping it leaves gateway state and the event queue unchanged.
    /// Risk and book checks run only when the token is applied.
    ///
    /// # Errors
    ///
    /// Returns a sequence error before checking event capacity. A full queue
    /// returns [`EventEngineError::Backpressured`].
    pub fn admit(
        &mut self,
        command: Command,
    ) -> Result<
        AdmittedCommand<
            '_,
            'queue,
            ACCOUNTS,
            RISK_ORDERS,
            LEVELS,
            ORDERS_PER_LEVEL,
            REPORTS,
            BATCH,
            QUEUE,
        >,
        EventEngineError,
    > {
        self.preflight(command.sequence())?;
        Ok(AdmittedCommand {
            engine: self,
            command,
        })
    }

    fn apply_admitted_command(&mut self, command: Command) -> Result<(), EventEngineError> {
        let sequence = command.sequence();
        let identity = CommandIdentity::from_command(command);
        let before = (
            self.gateway.top_level(Side::Buy),
            self.gateway.top_level(Side::Sell),
        );
        let mut reports = ReportBuffer::<REPORTS>::new();
        let result = self.gateway.process_command(command, &mut reports);
        let after = (
            self.gateway.top_level(Side::Buy),
            self.gateway.top_level(Side::Sell),
        );
        self.publish_result(identity, sequence, before, after, &reports, result)
    }

    fn preflight(&mut self, sequence: SequenceNumber) -> Result<(), EventEngineError> {
        self.gateway
            .check_sequence(sequence)
            .map_err(EventEngineError::Gateway)?;
        if !self.producer.has_capacity() {
            return Err(EventEngineError::Backpressured);
        }
        Ok(())
    }

    fn publish_result(
        &mut self,
        command: CommandIdentity,
        sequence: SequenceNumber,
        before: (Option<TopLevel>, Option<TopLevel>),
        after: (Option<TopLevel>, Option<TopLevel>),
        reports: &ReportBuffer<REPORTS>,
        result: Result<GatewayOutcome, GatewayError>,
    ) -> Result<(), EventEngineError> {
        let mut batch = EventBatch::new();

        match result {
            Ok(outcome) => {
                let mut ordinal = 0_u16;
                let terminal = terminal_event(outcome, command, sequence, ordinal);
                push_preflighted(&mut batch, terminal)?;
                ordinal += 1;
                for report in reports.iter() {
                    push_preflighted(
                        &mut batch,
                        Event::Trade(Trade {
                            id: EventId {
                                command_sequence: sequence,
                                ordinal,
                            },
                            maker_order_id: report.maker_order_id,
                            taker_order_id: report.taker_order_id,
                            instrument_id: report.instrument_id,
                            price: report.price,
                            quantity: report.quantity,
                        }),
                    )?;
                    ordinal += 1;
                }
                if before != after {
                    push_preflighted(
                        &mut batch,
                        Event::TopOfBook(TopOfBook {
                            id: EventId {
                                command_sequence: sequence,
                                ordinal,
                            },
                            instrument_id: command.instrument_id,
                            bid: after.0,
                            ask: after.1,
                        }),
                    )?;
                }
            }
            Err(GatewayError::Risk(reason) | GatewayError::Book(reason)) => {
                push_preflighted(
                    &mut batch,
                    Event::Rejected(Rejected {
                        id: EventId {
                            command_sequence: sequence,
                            ordinal: 0,
                        },
                        command: command.kind,
                        order_id: command.order_id,
                        account_id: command.account_id,
                        instrument_id: command.instrument_id,
                        reason,
                    }),
                )?;
            }
            Err(error) => return Err(EventEngineError::Gateway(error)),
        }

        // Capacity was observed before processing. Only the consumer can move
        // the peer cursor, and it can only free this producer's reserved slot.
        if self.producer.try_push(batch).is_err() {
            return Err(EventEngineError::PublicationInvariant);
        }
        Ok(())
    }

    #[must_use]
    pub const fn gateway(&self) -> &Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL> {
        &self.gateway
    }

    #[must_use]
    pub fn into_gateway(self) -> Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL> {
        self.gateway
    }
}

impl<
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const QUEUE: usize,
> AdmittedCommand<'_, '_, ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL, REPORTS, BATCH, QUEUE>
{
    /// Applies the admitted command and publishes its complete event batch.
    ///
    /// Sequence-valid business rejections publish a rejection event and
    /// return success.
    ///
    /// # Errors
    ///
    /// Returns fatal gateway or event publication errors. These failures can
    /// occur after mutation and must not be retried.
    pub fn apply(self) -> Result<(), EventEngineError> {
        self.engine.apply_admitted_command(self.command)
    }
}

#[derive(Clone, Copy)]
struct CommandIdentity {
    kind: CommandKind,
    order_id: OrderId,
    account_id: AccountId,
    instrument_id: InstrumentId,
}

impl CommandIdentity {
    fn from_message(message: BorrowedMessage<'_>) -> Self {
        match message {
            BorrowedMessage::NewOrder(message) => {
                let order = message.to_owned();
                Self {
                    kind: CommandKind::NewOrder,
                    order_id: order.order_id,
                    account_id: order.account_id,
                    instrument_id: order.instrument_id,
                }
            }
            BorrowedMessage::CancelOrder(message) => {
                let cancel = message.to_owned();
                Self {
                    kind: CommandKind::Cancel,
                    order_id: cancel.order_id,
                    account_id: cancel.account_id,
                    instrument_id: cancel.instrument_id,
                }
            }
            BorrowedMessage::ReplaceOrder(message) => {
                let replace = message.to_owned();
                Self {
                    kind: CommandKind::Replace,
                    order_id: replace.order_id,
                    account_id: replace.account_id,
                    instrument_id: replace.instrument_id,
                }
            }
        }
    }

    const fn from_command(command: Command) -> Self {
        match command {
            Command::NewOrder(order) => Self {
                kind: CommandKind::NewOrder,
                order_id: order.order_id,
                account_id: order.account_id,
                instrument_id: order.instrument_id,
            },
            Command::CancelOrder(cancel) => Self {
                kind: CommandKind::Cancel,
                order_id: cancel.order_id,
                account_id: cancel.account_id,
                instrument_id: cancel.instrument_id,
            },
            Command::ReplaceOrder(replace) => Self {
                kind: CommandKind::Replace,
                order_id: replace.order_id,
                account_id: replace.account_id,
                instrument_id: replace.instrument_id,
            },
        }
    }
}

fn terminal_event(
    outcome: GatewayOutcome,
    command: CommandIdentity,
    sequence: SequenceNumber,
    ordinal: u16,
) -> Event {
    let id = EventId {
        command_sequence: sequence,
        ordinal,
    };
    match outcome {
        GatewayOutcome::NewOrder(summary) => Event::Accepted(Accepted {
            id,
            order_id: command.order_id,
            account_id: command.account_id,
            instrument_id: command.instrument_id,
            state: summary.state,
            filled_quantity: summary.filled_quantity,
            resting_quantity: summary.resting_quantity,
            discarded_quantity: summary.discarded_quantity,
        }),
        GatewayOutcome::Cancelled(cancelled) => Event::Cancelled(Cancelled {
            id,
            order_id: cancelled.order_id,
            account_id: cancelled.account_id,
            instrument_id: command.instrument_id,
            quantity: cancelled.quantity,
        }),
        GatewayOutcome::Replaced(replaced) => Event::Replaced(Replaced {
            id,
            order_id: replaced.order_id,
            account_id: replaced.account_id,
            instrument_id: command.instrument_id,
            old_quantity: replaced.old_quantity,
            new_quantity: replaced.new_quantity,
            price: replaced.price,
            priority_lost: replaced.priority_lost,
        }),
    }
}

fn push_preflighted<const N: usize>(
    batch: &mut EventBatch<N>,
    event: Event,
) -> Result<(), EventEngineError> {
    batch
        .try_push(event)
        .map_err(|_| EventEngineError::EventCapacityInvariant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_risk::{RiskEngine, RiskLimits};
    use hft_spsc::SpscQueue;
    use hft_types::{CancelOrder, NewOrder, ReplaceOrder, TimeInForce};
    use hft_wire::{encode_cancel_order, encode_new_order, encode_replace_order};

    type TestGateway = Gateway<2, 8, 4, 4>;
    type TestBatch = EventBatch<6>;

    fn gateway() -> TestGateway {
        let limits = RiskLimits {
            max_quantity: Quantity(100),
            max_notional: 100_000,
            max_abs_position: Quantity(1_000),
            max_open_orders: 8,
            minimum_price: PriceTicks(1),
            maximum_price: PriceTicks(1_000),
        };
        let mut risk = RiskEngine::new();
        risk.register_account(AccountId(1), limits)
            .expect("account one");
        risk.register_account(AccountId(2), limits)
            .expect("account two");
        Gateway::new(risk, InstrumentId(7))
    }

    fn order(
        id: u64,
        account: u32,
        sequence: u64,
        side: Side,
        price: i64,
        quantity: u64,
    ) -> [u8; 46] {
        encode_new_order(NewOrder {
            order_id: OrderId(id),
            account_id: AccountId(account),
            instrument_id: InstrumentId(7),
            price: PriceTicks(price),
            quantity: Quantity(quantity),
            sequence: SequenceNumber(sequence),
            side,
            time_in_force: TimeInForce::Gtc,
        })
    }

    fn command(frame: &[u8]) -> Command {
        parse_message(&RxFrame::from_bytes(frame))
            .expect("command frame")
            .to_command()
    }

    #[test]
    fn accepted_trade_and_top_events_have_stable_order() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 4>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 4>::try_new(gateway(), producer)
            .expect("batch capacity");

        let sell = order(1, 1, 1, Side::Sell, 100, 5);
        engine
            .process_frame(&RxFrame::from_bytes(&sell))
            .expect("rest sell");
        let first = consumer.try_pop().expect("first batch");
        assert_eq!(first.len(), 2);
        assert!(matches!(first.iter().next(), Some(Event::Accepted(_))));
        assert!(matches!(first.iter().nth(1), Some(Event::TopOfBook(_))));

        let buy = order(2, 2, 2, Side::Buy, 100, 5);
        engine
            .process_frame(&RxFrame::from_bytes(&buy))
            .expect("cross sell");
        let second = consumer.try_pop().expect("second batch");
        let events: Vec<_> = second.iter().copied().collect();
        assert!(matches!(events[0], Event::Accepted(_)));
        assert!(matches!(events[1], Event::Trade(_)));
        assert!(matches!(events[2], Event::TopOfBook(_)));
        for (ordinal, event) in events.iter().enumerate() {
            let id = match event {
                Event::Accepted(event) => event.id,
                Event::Trade(event) => event.id,
                Event::TopOfBook(event) => event.id,
                _ => unreachable!("unexpected event"),
            };
            assert_eq!(id.command_sequence, SequenceNumber(2));
            assert_eq!(usize::from(id.ordinal), ordinal);
        }
        let Event::Trade(trade) = events[1] else {
            unreachable!("trade event checked above");
        };
        assert_eq!(trade.maker_order_id, OrderId(1));
        assert_eq!(trade.taker_order_id, OrderId(2));
        assert_eq!(trade.quantity, Quantity(5));
    }

    #[test]
    fn multi_level_trades_follow_book_price_time_order() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 4>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 4>::try_new(gateway(), producer)
            .expect("batch capacity");

        for frame in [
            order(1, 1, 1, Side::Sell, 100, 2),
            order(2, 1, 2, Side::Sell, 101, 3),
        ] {
            engine
                .process_frame(&RxFrame::from_bytes(&frame))
                .expect("rest maker");
            let _ = consumer.try_pop().expect("maker batch");
        }

        let taker = order(3, 2, 3, Side::Buy, 101, 5);
        engine
            .process_frame(&RxFrame::from_bytes(&taker))
            .expect("cross levels");
        let batch = consumer.try_pop().expect("taker batch");
        let trades: Vec<_> = batch
            .iter()
            .filter_map(|event| match event {
                Event::Trade(trade) => Some(*trade),
                _ => None,
            })
            .collect();

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].maker_order_id, OrderId(1));
        assert_eq!(trades[0].price, PriceTicks(100));
        assert_eq!(trades[1].maker_order_id, OrderId(2));
        assert_eq!(trades[1].price, PriceTicks(101));
        assert_eq!(trades[0].id.ordinal, 1);
        assert_eq!(trades[1].id.ordinal, 2);
    }

    #[test]
    fn replace_cancel_and_business_rejection_emit_terminal_events() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 8>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 8>::try_new(gateway(), producer)
            .expect("batch capacity");

        let first = order(1, 1, 1, Side::Buy, 100, 5);
        engine
            .process_frame(&RxFrame::from_bytes(&first))
            .expect("new order");
        let _ = consumer.try_pop();

        let replace = encode_replace_order(ReplaceOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(7),
            sequence: SequenceNumber(2),
            price: PriceTicks(100),
            quantity: Quantity(3),
        });
        engine
            .process_frame(&RxFrame::from_bytes(&replace))
            .expect("replace");
        let replaced = consumer.try_pop().expect("replace batch");
        assert!(matches!(replaced.iter().next(), Some(Event::Replaced(_))));
        assert!(matches!(replaced.iter().nth(1), Some(Event::TopOfBook(_))));

        let cancel = encode_cancel_order(CancelOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(7),
            sequence: SequenceNumber(3),
        });
        engine
            .process_frame(&RxFrame::from_bytes(&cancel))
            .expect("cancel");
        let cancelled = consumer.try_pop().expect("cancel batch");
        assert!(matches!(cancelled.iter().next(), Some(Event::Cancelled(_))));
        assert!(matches!(cancelled.iter().nth(1), Some(Event::TopOfBook(_))));

        let unknown = encode_cancel_order(CancelOrder {
            order_id: OrderId(99),
            account_id: AccountId(1),
            instrument_id: InstrumentId(7),
            sequence: SequenceNumber(4),
        });
        engine
            .process_frame(&RxFrame::from_bytes(&unknown))
            .expect("recorded rejection");
        let rejected = consumer.try_pop().expect("rejection batch");
        assert_eq!(rejected.len(), 1);
        assert!(matches!(
            rejected.iter().next(),
            Some(Event::Rejected(Rejected {
                command: CommandKind::Cancel,
                reason: RejectReason::UnknownOrder,
                ..
            }))
        ));
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(5));
    }

    #[test]
    fn rejected_new_and_replace_events_preserve_command_identity() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 2>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 2>::try_new(gateway(), producer)
            .expect("batch capacity");

        let rejected_new = order(7, 2, 1, Side::Buy, 100, 101);
        engine
            .process_frame(&RxFrame::from_bytes(&rejected_new))
            .expect("new rejection event");
        let new_batch = consumer.try_pop().expect("new rejection batch");
        assert_eq!(
            new_batch.iter().next(),
            Some(&Event::Rejected(Rejected {
                id: EventId {
                    command_sequence: SequenceNumber(1),
                    ordinal: 0,
                },
                command: CommandKind::NewOrder,
                order_id: OrderId(7),
                account_id: AccountId(2),
                instrument_id: InstrumentId(7),
                reason: RejectReason::QuantityLimit,
            }))
        );

        let rejected_replace = encode_replace_order(ReplaceOrder {
            order_id: OrderId(7),
            account_id: AccountId(2),
            instrument_id: InstrumentId(7),
            sequence: SequenceNumber(2),
            price: PriceTicks(101),
            quantity: Quantity(1),
        });
        engine
            .process_frame(&RxFrame::from_bytes(&rejected_replace))
            .expect("replace rejection event");
        let replace_batch = consumer.try_pop().expect("replace rejection batch");
        assert_eq!(
            replace_batch.iter().next(),
            Some(&Event::Rejected(Rejected {
                id: EventId {
                    command_sequence: SequenceNumber(2),
                    ordinal: 0,
                },
                command: CommandKind::Replace,
                order_id: OrderId(7),
                account_id: AccountId(2),
                instrument_id: InstrumentId(7),
                reason: RejectReason::UnknownOrder,
            }))
        );
    }

    #[test]
    fn full_queue_rejects_before_gateway_mutation_and_retry_succeeds() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(gateway(), producer)
            .expect("batch capacity");

        let first = order(1, 1, 1, Side::Buy, 99, 2);
        engine
            .process_frame(&RxFrame::from_bytes(&first))
            .expect("first command");
        let before = engine.gateway().export_state();
        let second = order(2, 1, 2, Side::Buy, 98, 2);
        assert_eq!(
            engine.process_frame(&RxFrame::from_bytes(&second)),
            Err(EventEngineError::Backpressured)
        );
        assert_eq!(engine.gateway().export_state(), before);
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(2));

        let _ = consumer.try_pop().expect("free queue slot");
        engine
            .process_frame(&RxFrame::from_bytes(&second))
            .expect("retry");
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(3));
        assert!(consumer.try_pop().is_some());
    }

    #[test]
    fn dropped_admission_preserves_state_and_can_be_retried() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(gateway(), producer)
            .expect("batch capacity");
        let new_order = command(&order(1, 1, 1, Side::Buy, 99, 2));
        let before = engine.gateway().export_state();

        {
            let _admitted = engine.admit(new_order).expect("admit command");
            assert!(consumer.try_pop().is_none());
        }

        assert_eq!(engine.gateway().export_state(), before);
        assert!(consumer.try_pop().is_none());
        engine
            .admit(new_order)
            .expect("retry admission")
            .apply()
            .expect("apply command");
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(2));
        let batch = consumer.try_pop().expect("command batch");
        assert_eq!(batch.len(), 2);
        assert!(matches!(
            batch.iter().next(),
            Some(Event::Accepted(Accepted {
                id: EventId {
                    command_sequence: SequenceNumber(1),
                    ordinal: 0,
                },
                order_id: OrderId(1),
                resting_quantity: Quantity(2),
                ..
            }))
        ));
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn admission_checks_sequence_then_capacity_before_mutation() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(gateway(), producer)
            .expect("batch capacity");
        engine
            .process_command(command(&order(1, 1, 1, Side::Buy, 99, 2)))
            .expect("first command");
        let before = engine.gateway().export_state();

        let gap = command(&order(2, 1, 3, Side::Buy, 98, 2));
        assert!(matches!(
            engine.admit(gap),
            Err(EventEngineError::Gateway(GatewayError::Sequence {
                expected: SequenceNumber(2),
                received: SequenceNumber(3),
            }))
        ));
        let second = command(&order(2, 1, 2, Side::Buy, 98, 2));
        assert!(matches!(
            engine.admit(second),
            Err(EventEngineError::Backpressured)
        ));
        assert_eq!(engine.gateway().export_state(), before);

        let _ = consumer.try_pop().expect("first command batch");
        assert!(consumer.try_pop().is_none());
        engine
            .admit(second)
            .expect("admit after reclaim")
            .apply()
            .expect("second command");
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(3));
        assert!(consumer.try_pop().is_some());
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn admission_checks_sequence_exhaustion_before_capacity() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut state = gateway().export_state();
        state.expected_sequence = SequenceNumber(u64::MAX);
        let exhausted = Gateway::from_state(&state).expect("boundary state");
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (mut producer, mut consumer) = queue.split();
        producer.try_push(TestBatch::new()).expect("fill queue");
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(exhausted, producer)
            .expect("batch capacity");
        let last = command(&order(1, 1, u64::MAX, Side::Buy, 100, 1));

        assert!(matches!(
            engine.admit(last),
            Err(EventEngineError::Gateway(GatewayError::RiskState(
                RejectReason::ArithmeticOverflow
            )))
        ));
        assert_eq!(engine.gateway().export_state(), state);
        assert_eq!(consumer.try_pop(), Some(TestBatch::new()));
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn admitted_business_rejection_is_published_only_when_applied() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(gateway(), producer)
            .expect("batch capacity");
        let oversized = command(&order(7, 2, 1, Side::Buy, 100, 101));
        let admitted = engine.admit(oversized).expect("admit command");
        assert!(consumer.try_pop().is_none());
        admitted.apply().expect("publish business rejection");

        let batch = consumer.try_pop().expect("rejection batch");
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch.iter().next(),
            Some(&Event::Rejected(Rejected {
                id: EventId {
                    command_sequence: SequenceNumber(1),
                    ordinal: 0,
                },
                command: CommandKind::NewOrder,
                order_id: OrderId(7),
                account_id: AccountId(2),
                instrument_id: InstrumentId(7),
                reason: RejectReason::QuantityLimit,
            }))
        );
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(2));
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn parse_and_sequence_errors_publish_nothing() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<TestBatch, 2>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 2>::try_new(gateway(), producer)
            .expect("batch capacity");

        assert!(matches!(
            engine.process_frame(&RxFrame::from_bytes(&[0_u8; 1])),
            Err(EventEngineError::Gateway(GatewayError::Parse(_)))
        ));
        let gap = order(1, 1, 2, Side::Buy, 100, 1);
        assert!(matches!(
            engine.process_frame(&RxFrame::from_bytes(&gap)),
            Err(EventEngineError::Gateway(GatewayError::Sequence { .. }))
        ));
        assert!(consumer.try_pop().is_none());
        assert_eq!(engine.gateway().expected_sequence(), SequenceNumber(1));
    }

    #[test]
    fn sequence_exhaustion_publishes_nothing() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut state = gateway().export_state();
        state.expected_sequence = SequenceNumber(u64::MAX);
        let exhausted = Gateway::from_state(&state).expect("boundary state");
        let mut queue = SpscQueue::<TestBatch, 1>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 1>::try_new(exhausted, producer)
            .expect("batch capacity");
        let last = order(1, 1, u64::MAX, Side::Buy, 100, 1);

        assert_eq!(
            engine.process_frame(&RxFrame::from_bytes(&last)),
            Err(EventEngineError::Gateway(GatewayError::RiskState(
                RejectReason::ArithmeticOverflow
            )))
        );
        assert!(consumer.try_pop().is_none());
        assert_eq!(
            engine.gateway().expected_sequence(),
            SequenceNumber(u64::MAX)
        );
    }

    #[test]
    fn undersized_batch_is_rejected_at_construction() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<EventBatch<5>, 1>::try_new().expect("queue");
        let (producer, _consumer) = queue.split();
        assert!(matches!(
            BoundedEventEngine::<2, 8, 4, 4, 4, 5, 1>::try_new(gateway(), producer),
            Err(EventEngineConfigError::BatchTooSmall)
        ));
    }

    #[test]
    fn event_capacity_limits_are_explicit() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut queue = SpscQueue::<EventBatch<1>, 1>::try_new().expect("queue");
        let (producer, _consumer) = queue.split();
        assert!(matches!(
            BoundedEventEngine::<2, 8, 4, 4, 65_535, 1, 1>::try_new(gateway(), producer),
            Err(EventEngineConfigError::TooManyEvents)
        ));

        let event = Event::Rejected(Rejected {
            id: EventId {
                command_sequence: SequenceNumber(1),
                ordinal: 0,
            },
            command: CommandKind::NewOrder,
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(7),
            reason: RejectReason::QuantityLimit,
        });
        let mut batch = EventBatch::<1>::new();
        assert_eq!(batch.try_push(event), Ok(()));
        assert_eq!(batch.try_push(event), Err(BatchFull));
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn replay_and_snapshot_tail_emit_identical_event_batches() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let frames = [
            order(1, 1, 1, Side::Sell, 100, 5),
            order(2, 2, 2, Side::Buy, 100, 2),
            order(2, 2, 3, Side::Buy, 100, 1),
            order(3, 2, 4, Side::Buy, 100, 3),
        ];

        let full = event_batches(gateway(), &frames);
        assert_eq!(full.len(), frames.len());
        assert_eq!(event_batches(gateway(), &frames), full);

        let mut prefix = gateway();
        let mut reports = ReportBuffer::<4>::new();
        for frame in &frames[..2] {
            let _ = prefix.process_frame(&RxFrame::from_bytes(frame), &mut reports);
        }
        let snapshot = hft_recovery::encode_snapshot(&prefix, 2).expect("snapshot");
        let restored = hft_recovery::decode_snapshot::<2, 8, 4, 4>(snapshot.bytes())
            .expect("restore")
            .gateway;
        let tail = event_batches(restored, &frames[2..]);

        assert_eq!(tail, full[2..]);
    }

    fn event_batches(gateway: TestGateway, frames: &[[u8; 46]]) -> Vec<TestBatch> {
        let mut queue = SpscQueue::<TestBatch, 8>::try_new().expect("queue");
        let (producer, mut consumer) = queue.split();
        let mut engine = BoundedEventEngine::<2, 8, 4, 4, 4, 6, 8>::try_new(gateway, producer)
            .expect("batch capacity");
        let mut batches = Vec::with_capacity(frames.len());
        for frame in frames {
            engine
                .process_frame(&RxFrame::from_bytes(frame))
                .expect("replay command");
            batches.push(consumer.try_pop().expect("event batch"));
        }
        batches
    }
}
