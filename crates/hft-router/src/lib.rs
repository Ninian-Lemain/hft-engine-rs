#![forbid(unsafe_code)]

use hft_events::{BoundedEventEngine, EventBatch, EventEngineConfigError, EventEngineError};
use hft_gateway::Gateway;
use hft_io::RxFrame;
use hft_spsc::{Consumer, Producer};
use hft_types::{Command, InstrumentId, SequenceNumber};
use hft_wire::{ParseError, parse_message};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ShardId(pub u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentRoute {
    pub instrument_id: InstrumentId,
    pub shard_id: ShardId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteTableError {
    NoShards,
    TooManyShards,
    InvalidShardId(ShardId),
    DuplicateShardId(ShardId),
    DuplicateInstrument(InstrumentId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteTable<const SHARDS: usize> {
    by_instrument: [InstrumentRoute; SHARDS],
    by_shard: [u16; SHARDS],
    dense: bool,
}

impl<const SHARDS: usize> RouteTable<SHARDS> {
    /// Builds a route table with exactly one instrument per shard.
    ///
    /// Input order does not affect lookup results. Routes are stored in
    /// instrument order and shard IDs must cover `0..SHARDS` exactly once.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized tables, invalid or duplicate shard IDs, and
    /// duplicate instruments.
    pub fn try_new(mut routes: [InstrumentRoute; SHARDS]) -> Result<Self, RouteTableError> {
        if SHARDS == 0 {
            return Err(RouteTableError::NoShards);
        }
        if SHARDS > usize::from(u16::MAX) + 1 {
            return Err(RouteTableError::TooManyShards);
        }

        let mut seen_shards = [false; SHARDS];
        for route in routes {
            let shard = usize::from(route.shard_id.0);
            if shard >= SHARDS {
                return Err(RouteTableError::InvalidShardId(route.shard_id));
            }
            if seen_shards[shard] {
                return Err(RouteTableError::DuplicateShardId(route.shard_id));
            }
            seen_shards[shard] = true;
        }

        routes.sort_unstable_by_key(|route| route.instrument_id);
        for pair in routes.windows(2) {
            if pair[0].instrument_id == pair[1].instrument_id {
                return Err(RouteTableError::DuplicateInstrument(pair[0].instrument_id));
            }
        }
        let mut by_shard = [0; SHARDS];
        for (index, route) in routes.iter().enumerate() {
            by_shard[usize::from(route.shard_id.0)] =
                u16::try_from(index).map_err(|_| RouteTableError::TooManyShards)?;
        }
        let dense = routes[SHARDS - 1].instrument_id.0 - routes[0].instrument_id.0
            == u32::try_from(SHARDS - 1).map_err(|_| RouteTableError::TooManyShards)?;

        Ok(Self {
            by_instrument: routes,
            by_shard,
            dense,
        })
    }

    #[must_use]
    pub fn shard_for(&self, instrument_id: InstrumentId) -> Option<ShardId> {
        if self.dense {
            let offset = instrument_id
                .0
                .checked_sub(self.by_instrument[0].instrument_id.0)?;
            return self
                .by_instrument
                .get(usize::try_from(offset).ok()?)
                .map(|route| route.shard_id);
        }
        self.by_instrument
            .binary_search_by_key(&instrument_id, |route| route.instrument_id)
            .ok()
            .map(|index| self.by_instrument[index].shard_id)
    }

    #[must_use]
    pub fn instrument_for(&self, shard_id: ShardId) -> Option<InstrumentId> {
        let index = *self.by_shard.get(usize::from(shard_id.0))?;
        self.by_instrument
            .get(usize::from(index))
            .map(|route| route.instrument_id)
    }

    #[must_use]
    pub const fn routes(&self) -> &[InstrumentRoute; SHARDS] {
        &self.by_instrument
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouterError {
    Parse(ParseError),
    UnknownInstrument(InstrumentId),
    UnknownShard(ShardId),
    CommandBackpressured(ShardId),
}

pub struct MultiInstrumentRouter<
    'command,
    'event,
    const SHARDS: usize,
    const BATCH: usize,
    const COMMAND_QUEUE: usize,
    const EVENT_QUEUE: usize,
> {
    routes: RouteTable<SHARDS>,
    command_producers: [Producer<'command, Command, COMMAND_QUEUE>; SHARDS],
    event_consumers: [Consumer<'event, EventBatch<BATCH>, EVENT_QUEUE>; SHARDS],
}

impl<
    'command,
    'event,
    const SHARDS: usize,
    const BATCH: usize,
    const COMMAND_QUEUE: usize,
    const EVENT_QUEUE: usize,
> MultiInstrumentRouter<'command, 'event, SHARDS, BATCH, COMMAND_QUEUE, EVENT_QUEUE>
{
    /// Queue arrays are indexed by [`ShardId`].
    #[must_use]
    pub const fn new(
        routes: RouteTable<SHARDS>,
        command_producers: [Producer<'command, Command, COMMAND_QUEUE>; SHARDS],
        event_consumers: [Consumer<'event, EventBatch<BATCH>, EVENT_QUEUE>; SHARDS],
    ) -> Self {
        Self {
            routes,
            command_producers,
            event_consumers,
        }
    }

    /// Parses and publishes one command to its configured shard.
    ///
    /// # Errors
    ///
    /// Returns a parse or unknown-instrument rejection before publication.
    /// A full shard queue returns [`RouterError::CommandBackpressured`].
    pub fn route_frame(&mut self, frame: &RxFrame<'_>) -> Result<ShardId, RouterError> {
        let command = parse_message(frame)
            .map_err(RouterError::Parse)?
            .to_command();
        self.route_command(command)
    }

    /// Publishes one normalized command to its configured shard.
    ///
    /// # Errors
    ///
    /// Returns an unknown-instrument rejection or explicit queue backpressure.
    pub fn route_command(&mut self, command: Command) -> Result<ShardId, RouterError> {
        let instrument_id = command.instrument_id();
        let shard_id = self
            .routes
            .shard_for(instrument_id)
            .ok_or(RouterError::UnknownInstrument(instrument_id))?;
        let producer = self
            .command_producers
            .get_mut(usize::from(shard_id.0))
            .ok_or(RouterError::UnknownShard(shard_id))?;
        producer
            .try_push(command)
            .map_err(|_| RouterError::CommandBackpressured(shard_id))?;
        Ok(shard_id)
    }

    /// Pops the next event batch from one shard without merging shard order.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::UnknownShard`] for an out-of-range shard ID.
    pub fn try_event(
        &mut self,
        shard_id: ShardId,
    ) -> Result<Option<EventBatch<BATCH>>, RouterError> {
        self.event_consumers
            .get_mut(usize::from(shard_id.0))
            .ok_or(RouterError::UnknownShard(shard_id))
            .map(Consumer::try_pop)
    }

    #[must_use]
    pub const fn routes(&self) -> &RouteTable<SHARDS> {
        &self.routes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardConfigError {
    InstrumentMismatch {
        route: InstrumentId,
        gateway: InstrumentId,
    },
    Events(EventEngineConfigError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardStep {
    Idle,
    Processed(SequenceNumber),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardError {
    Misrouted {
        expected: InstrumentId,
        received: InstrumentId,
    },
    EventBackpressured,
    Engine(EventEngineError),
}

pub struct MatchingShard<
    'command,
    'event,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const COMMAND_QUEUE: usize,
    const EVENT_QUEUE: usize,
> {
    route: InstrumentRoute,
    command_consumer: Consumer<'command, Command, COMMAND_QUEUE>,
    engine: BoundedEventEngine<
        'event,
        ACCOUNTS,
        RISK_ORDERS,
        LEVELS,
        ORDERS_PER_LEVEL,
        REPORTS,
        BATCH,
        EVENT_QUEUE,
    >,
    pending: Option<Command>,
}

impl<
    'command,
    'event,
    const ACCOUNTS: usize,
    const RISK_ORDERS: usize,
    const LEVELS: usize,
    const ORDERS_PER_LEVEL: usize,
    const REPORTS: usize,
    const BATCH: usize,
    const COMMAND_QUEUE: usize,
    const EVENT_QUEUE: usize,
>
    MatchingShard<
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
    >
{
    /// Builds one single-writer shard from its exclusive queue endpoints.
    ///
    /// # Errors
    ///
    /// Rejects a route and gateway instrument mismatch or an event batch that
    /// cannot hold the configured report count.
    pub fn try_new(
        route: InstrumentRoute,
        gateway: Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL>,
        command_consumer: Consumer<'command, Command, COMMAND_QUEUE>,
        event_producer: Producer<'event, EventBatch<BATCH>, EVENT_QUEUE>,
    ) -> Result<Self, ShardConfigError> {
        let gateway_instrument = gateway.instrument();
        if route.instrument_id != gateway_instrument {
            return Err(ShardConfigError::InstrumentMismatch {
                route: route.instrument_id,
                gateway: gateway_instrument,
            });
        }
        let engine = BoundedEventEngine::try_new(gateway, event_producer)
            .map_err(ShardConfigError::Events)?;
        Ok(Self {
            route,
            command_consumer,
            engine,
            pending: None,
        })
    }

    /// Processes at most one queued command.
    ///
    /// Event backpressure retains the command for the next call. All other
    /// failures return an explicit error.
    ///
    /// # Errors
    ///
    /// Returns a misroute, event backpressure, or engine failure without
    /// silently dropping an accepted command.
    pub fn try_process_one(&mut self) -> Result<ShardStep, ShardError> {
        let Some(command) = self
            .pending
            .take()
            .or_else(|| self.command_consumer.try_pop())
        else {
            return Ok(ShardStep::Idle);
        };
        let received = command.instrument_id();
        if received != self.route.instrument_id {
            return Err(ShardError::Misrouted {
                expected: self.route.instrument_id,
                received,
            });
        }
        let sequence = command.sequence();
        match self.engine.process_command(command) {
            Ok(()) => Ok(ShardStep::Processed(sequence)),
            Err(EventEngineError::Backpressured) => {
                self.pending = Some(command);
                Err(ShardError::EventBackpressured)
            }
            Err(error) => Err(ShardError::Engine(error)),
        }
    }

    #[must_use]
    pub const fn route(&self) -> InstrumentRoute {
        self.route
    }

    #[must_use]
    pub const fn has_pending_command(&self) -> bool {
        self.pending.is_some()
    }

    #[must_use]
    pub const fn gateway(&self) -> &Gateway<ACCOUNTS, RISK_ORDERS, LEVELS, ORDERS_PER_LEVEL> {
        self.engine.gateway()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_events::Event;
    use hft_risk::{RiskEngine, RiskLimits};
    use hft_spsc::SpscQueue;
    use hft_types::{AccountId, NewOrder, OrderId, PriceTicks, Quantity, Side, TimeInForce};
    use hft_wire::encode_new_order;

    const FIRST: InstrumentRoute = InstrumentRoute {
        instrument_id: InstrumentId(11),
        shard_id: ShardId(0),
    };
    const SECOND: InstrumentRoute = InstrumentRoute {
        instrument_id: InstrumentId(22),
        shard_id: ShardId(1),
    };

    type TestGateway = Gateway<2, 8, 4, 4>;
    type TestBatch = EventBatch<6>;

    fn gateway(instrument_id: InstrumentId) -> TestGateway {
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
        Gateway::new(risk, instrument_id)
    }

    const fn order(id: u64, instrument_id: InstrumentId, sequence: u64) -> NewOrder {
        NewOrder {
            order_id: OrderId(id),
            account_id: AccountId(1),
            instrument_id,
            price: PriceTicks(100),
            quantity: Quantity(1),
            sequence: SequenceNumber(sequence),
            side: Side::Sell,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn route_table_is_stable_and_rejects_invalid_shapes() {
        let forward = RouteTable::try_new([FIRST, SECOND]).expect("forward routes");
        let reverse = RouteTable::try_new([SECOND, FIRST]).expect("reverse routes");
        assert_eq!(forward, reverse);
        assert_eq!(forward.shard_for(InstrumentId(11)), Some(ShardId(0)));
        assert_eq!(forward.shard_for(InstrumentId(22)), Some(ShardId(1)));
        assert_eq!(forward.shard_for(InstrumentId(33)), None);
        assert_eq!(forward.instrument_for(ShardId(1)), Some(InstrumentId(22)));
        assert_eq!(forward.instrument_for(ShardId(2)), None);

        assert_eq!(RouteTable::<0>::try_new([]), Err(RouteTableError::NoShards));
        assert_eq!(
            RouteTable::try_new([
                FIRST,
                InstrumentRoute {
                    instrument_id: InstrumentId(22),
                    shard_id: ShardId(0),
                },
            ]),
            Err(RouteTableError::DuplicateShardId(ShardId(0)))
        );
        assert_eq!(
            RouteTable::try_new([
                FIRST,
                InstrumentRoute {
                    instrument_id: InstrumentId(11),
                    shard_id: ShardId(1),
                },
            ]),
            Err(RouteTableError::DuplicateInstrument(InstrumentId(11)))
        );
        assert_eq!(
            RouteTable::try_new([
                FIRST,
                InstrumentRoute {
                    instrument_id: InstrumentId(22),
                    shard_id: ShardId(2),
                },
            ]),
            Err(RouteTableError::InvalidShardId(ShardId(2)))
        );
    }

    #[test]
    fn dense_routes_resolve_offsets_without_changing_canonical_order() {
        let routes = [
            InstrumentRoute {
                instrument_id: InstrumentId(103),
                shard_id: ShardId(0),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(100),
                shard_id: ShardId(2),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(102),
                shard_id: ShardId(1),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(101),
                shard_id: ShardId(3),
            },
        ];
        let table = RouteTable::try_new(routes).expect("dense routes");
        for route in routes {
            assert_eq!(table.shard_for(route.instrument_id), Some(route.shard_id));
            assert_eq!(
                table.instrument_for(route.shard_id),
                Some(route.instrument_id)
            );
        }
        assert_eq!(table.shard_for(InstrumentId(99)), None);
        assert_eq!(table.shard_for(InstrumentId(104)), None);
        assert_eq!(
            table.routes().map(|route| route.instrument_id),
            [
                InstrumentId(100),
                InstrumentId(101),
                InstrumentId(102),
                InstrumentId(103)
            ]
        );
    }

    #[test]
    fn sparse_routes_search_displaced_ids_and_reject_holes() {
        let routes = [
            InstrumentRoute {
                instrument_id: InstrumentId(10),
                shard_id: ShardId(2),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(11),
                shard_id: ShardId(4),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(13),
                shard_id: ShardId(1),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(900),
                shard_id: ShardId(0),
            },
            InstrumentRoute {
                instrument_id: InstrumentId(u32::MAX),
                shard_id: ShardId(3),
            },
        ];
        let table = RouteTable::try_new(routes).expect("sparse routes");
        for id in (0..=1_000).chain([u32::MAX - 1, u32::MAX]) {
            let expected = routes
                .iter()
                .find(|route| route.instrument_id.0 == id)
                .map(|route| route.shard_id);
            assert_eq!(table.shard_for(InstrumentId(id)), expected);
        }
        for route in routes {
            assert_eq!(
                table.instrument_for(route.shard_id),
                Some(route.instrument_id)
            );
        }
    }

    #[test]
    fn dense_routes_preserve_instrument_id_boundaries() {
        for start in [0, u32::MAX - 2] {
            let table =
                RouteTable::try_new(std::array::from_fn::<_, 3, _>(|index| InstrumentRoute {
                    instrument_id: InstrumentId(start + u32::try_from(index).expect("index")),
                    shard_id: ShardId(u16::try_from(2 - index).expect("shard")),
                }))
                .expect("boundary routes");
            for index in 0_u16..3 {
                let instrument_id = InstrumentId(start + u32::from(index));
                let shard_id = ShardId(2 - index);
                assert_eq!(table.shard_for(instrument_id), Some(shard_id));
                assert_eq!(table.instrument_for(shard_id), Some(instrument_id));
            }
            assert_eq!(table.shard_for(InstrumentId(start.wrapping_sub(1))), None);
            assert_eq!(table.shard_for(InstrumentId(start.wrapping_add(3))), None);
        }
    }

    #[test]
    fn reverse_route_index_covers_every_shard_id() {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                const SHARDS: usize = 65_536;
                let routes = std::array::from_fn::<_, SHARDS, _>(|index| InstrumentRoute {
                    instrument_id: InstrumentId(u32::MAX - u32::try_from(index).expect("index")),
                    shard_id: ShardId(u16::try_from(index).expect("shard")),
                });
                let table = RouteTable::try_new(routes).expect("maximum shard count");
                for shard in 0..=u16::MAX {
                    let instrument_id = InstrumentId(u32::MAX - u32::from(shard));
                    assert_eq!(table.instrument_for(ShardId(shard)), Some(instrument_id));
                    assert_eq!(table.shard_for(instrument_id), Some(ShardId(shard)));
                }
            })
            .expect("test thread")
            .join()
            .expect("maximum shard count test");
    }

    #[test]
    fn reverse_route_index_uses_two_bytes_per_shard() {
        assert_eq!(core::mem::size_of::<RouteTable<4>>(), 44);
        assert_eq!(core::mem::size_of::<RouteTable<64>>(), 644);
        assert_eq!(core::mem::size_of::<RouteTable<1024>>(), 10_244);
    }

    #[test]
    fn commands_and_events_stay_on_their_configured_shards() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut command_zero = SpscQueue::<Command, 2>::try_new().expect("command zero");
        let mut command_one = SpscQueue::<Command, 2>::try_new().expect("command one");
        let (command_zero_producer, command_zero_consumer) = command_zero.split();
        let (command_one_producer, command_one_consumer) = command_one.split();
        let mut event_zero = SpscQueue::<TestBatch, 2>::try_new().expect("event zero");
        let mut event_one = SpscQueue::<TestBatch, 2>::try_new().expect("event one");
        let (event_zero_producer, event_zero_consumer) = event_zero.split();
        let (event_one_producer, event_one_consumer) = event_one.split();
        let route_table = RouteTable::try_new([SECOND, FIRST]).expect("routes");
        let mut router = MultiInstrumentRouter::new(
            route_table,
            [command_zero_producer, command_one_producer],
            [event_zero_consumer, event_one_consumer],
        );
        let mut shard_zero = MatchingShard::<2, 8, 4, 4, 4, 6, 2, 2>::try_new(
            FIRST,
            gateway(FIRST.instrument_id),
            command_zero_consumer,
            event_zero_producer,
        )
        .expect("shard zero");
        let mut shard_one = MatchingShard::<2, 8, 4, 4, 4, 6, 2, 2>::try_new(
            SECOND,
            gateway(SECOND.instrument_id),
            command_one_consumer,
            event_one_producer,
        )
        .expect("shard one");

        let shard_one_before = shard_one.gateway().stable_digest();
        let first_frame = encode_new_order(order(1, FIRST.instrument_id, 1));
        assert_eq!(
            router.route_frame(&RxFrame::from_bytes(&first_frame)),
            Ok(ShardId(0))
        );
        assert_eq!(
            shard_zero.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(1)))
        );
        assert_eq!(shard_one.gateway().stable_digest(), shard_one_before);
        assert_eq!(shard_one.try_process_one(), Ok(ShardStep::Idle));

        let second_frame = encode_new_order(order(1, SECOND.instrument_id, 1));
        assert_eq!(
            router.route_frame(&RxFrame::from_bytes(&second_frame)),
            Ok(ShardId(1))
        );
        assert_eq!(
            shard_one.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(1)))
        );

        for shard_id in [ShardId(0), ShardId(1)] {
            let batch = router
                .try_event(shard_id)
                .expect("known shard")
                .expect("event batch");
            assert!(batch.iter().all(|event| {
                match event {
                    Event::Accepted(event) => {
                        event.instrument_id
                            == router
                                .routes()
                                .instrument_for(shard_id)
                                .expect("instrument")
                    }
                    Event::TopOfBook(event) => {
                        event.instrument_id
                            == router
                                .routes()
                                .instrument_for(shard_id)
                                .expect("instrument")
                    }
                    _ => false,
                }
            }));
        }
    }

    #[test]
    fn unknown_instrument_and_full_command_queue_reject() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut command_zero = SpscQueue::<Command, 1>::try_new().expect("command zero");
        let mut command_one = SpscQueue::<Command, 1>::try_new().expect("command one");
        let (command_zero_producer, mut command_zero_consumer) = command_zero.split();
        let (command_one_producer, mut command_one_consumer) = command_one.split();
        let mut event_zero = SpscQueue::<TestBatch, 1>::try_new().expect("event zero");
        let mut event_one = SpscQueue::<TestBatch, 1>::try_new().expect("event one");
        let (_, event_zero_consumer) = event_zero.split();
        let (_, event_one_consumer) = event_one.split();
        let mut router = MultiInstrumentRouter::new(
            RouteTable::try_new([FIRST, SECOND]).expect("routes"),
            [command_zero_producer, command_one_producer],
            [event_zero_consumer, event_one_consumer],
        );

        let unknown = Command::NewOrder(order(1, InstrumentId(33), 1));
        assert_eq!(
            router.route_command(unknown),
            Err(RouterError::UnknownInstrument(InstrumentId(33)))
        );
        let first = Command::NewOrder(order(1, FIRST.instrument_id, 1));
        let second = Command::NewOrder(order(2, FIRST.instrument_id, 2));
        assert_eq!(router.route_command(first), Ok(ShardId(0)));
        assert_eq!(
            router.route_command(second),
            Err(RouterError::CommandBackpressured(ShardId(0)))
        );
        assert_eq!(command_zero_consumer.try_pop(), Some(first));
        assert_eq!(command_zero_consumer.try_pop(), None);
        assert_eq!(command_one_consumer.try_pop(), None);
    }

    #[test]
    fn event_backpressure_retains_the_command_for_retry() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut command = SpscQueue::<Command, 2>::try_new().expect("command queue");
        let (mut command_producer, command_consumer) = command.split();
        let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
        let (event_producer, mut event_consumer) = events.split();
        let mut shard = MatchingShard::<2, 8, 4, 4, 4, 6, 2, 1>::try_new(
            FIRST,
            gateway(FIRST.instrument_id),
            command_consumer,
            event_producer,
        )
        .expect("shard");

        assert_eq!(
            command_producer.try_push(Command::NewOrder(order(1, FIRST.instrument_id, 1))),
            Ok(())
        );
        assert_eq!(
            shard.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(1)))
        );
        assert_eq!(
            command_producer.try_push(Command::NewOrder(order(2, FIRST.instrument_id, 2))),
            Ok(())
        );
        assert_eq!(shard.try_process_one(), Err(ShardError::EventBackpressured));
        assert!(shard.has_pending_command());
        assert_eq!(shard.gateway().expected_sequence(), SequenceNumber(2));

        assert!(event_consumer.try_pop().is_some());
        assert_eq!(
            shard.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(2)))
        );
        assert!(!shard.has_pending_command());
        assert_eq!(shard.gateway().expected_sequence(), SequenceNumber(3));
    }

    #[test]
    fn pending_command_retries_before_the_next_queued_command() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut commands = SpscQueue::<Command, 2>::try_new().expect("command queue");
        let (mut command_producer, command_consumer) = commands.split();
        let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
        let (event_producer, mut event_consumer) = events.split();
        let mut shard = MatchingShard::<2, 8, 4, 4, 4, 6, 2, 1>::try_new(
            FIRST,
            gateway(FIRST.instrument_id),
            command_consumer,
            event_producer,
        )
        .expect("shard");

        command_producer
            .try_push(Command::NewOrder(order(1, FIRST.instrument_id, 1)))
            .expect("seed command");
        assert_eq!(
            shard.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(1)))
        );

        command_producer
            .try_push(Command::NewOrder(order(2, FIRST.instrument_id, 2)))
            .expect("command A");
        command_producer
            .try_push(Command::NewOrder(order(3, FIRST.instrument_id, 3)))
            .expect("command B");
        for _ in 0..2 {
            assert_eq!(shard.try_process_one(), Err(ShardError::EventBackpressured));
            assert!(shard.has_pending_command());
            assert_eq!(shard.gateway().expected_sequence(), SequenceNumber(2));
        }

        assert!(event_consumer.try_pop().is_some());
        assert_eq!(
            shard.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(2)))
        );
        let command_a = event_consumer.try_pop().expect("command A events");
        assert_eq!(command_a.len(), 2);
        assert!(command_a.iter().all(|event| match event {
            Event::Accepted(event) => event.id.command_sequence == SequenceNumber(2),
            Event::TopOfBook(event) => event.id.command_sequence == SequenceNumber(2),
            _ => false,
        }));
        assert!(event_consumer.try_pop().is_none());

        assert_eq!(
            shard.try_process_one(),
            Ok(ShardStep::Processed(SequenceNumber(3)))
        );
        let command_b = event_consumer.try_pop().expect("command B events");
        assert_eq!(command_b.len(), 2);
        assert!(command_b.iter().all(|event| match event {
            Event::Accepted(event) => event.id.command_sequence == SequenceNumber(3),
            Event::TopOfBook(event) => event.id.command_sequence == SequenceNumber(3),
            _ => false,
        }));
        assert!(event_consumer.try_pop().is_none());
        assert!(!shard.has_pending_command());
        assert_eq!(shard.gateway().expected_sequence(), SequenceNumber(4));
        assert_eq!(shard.try_process_one(), Ok(ShardStep::Idle));
    }

    #[test]
    fn saturated_shard_does_not_block_the_other_shard() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut command_zero = SpscQueue::<Command, 1>::try_new().expect("command zero");
        let mut command_one = SpscQueue::<Command, 1>::try_new().expect("command one");
        let (command_zero_producer, mut command_zero_consumer) = command_zero.split();
        let (command_one_producer, mut command_one_consumer) = command_one.split();
        let mut event_zero = SpscQueue::<TestBatch, 1>::try_new().expect("event zero");
        let mut event_one = SpscQueue::<TestBatch, 1>::try_new().expect("event one");
        let (_, event_zero_consumer) = event_zero.split();
        let (_, event_one_consumer) = event_one.split();
        let mut router = MultiInstrumentRouter::new(
            RouteTable::try_new([FIRST, SECOND]).expect("routes"),
            [command_zero_producer, command_one_producer],
            [event_zero_consumer, event_one_consumer],
        );

        let blocked = Command::NewOrder(order(1, FIRST.instrument_id, 1));
        assert_eq!(router.route_command(blocked), Ok(ShardId(0)));
        assert_eq!(
            router.route_command(Command::NewOrder(order(2, FIRST.instrument_id, 2))),
            Err(RouterError::CommandBackpressured(ShardId(0)))
        );

        for sequence in 1..=32 {
            let command = Command::NewOrder(order(sequence, SECOND.instrument_id, sequence));
            assert_eq!(router.route_command(command), Ok(ShardId(1)));
            assert_eq!(command_one_consumer.try_pop(), Some(command));
        }
        assert_eq!(command_zero_consumer.try_pop(), Some(blocked));
        assert_eq!(command_zero_consumer.try_pop(), None);
        assert_eq!(command_one_consumer.try_pop(), None);
    }

    #[test]
    fn shard_constructor_rejects_instrument_mismatch() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let mut command = SpscQueue::<Command, 1>::try_new().expect("command queue");
        let (_, command_consumer) = command.split();
        let mut events = SpscQueue::<TestBatch, 1>::try_new().expect("event queue");
        let (event_producer, _) = events.split();
        let result = MatchingShard::<2, 8, 4, 4, 4, 6, 1, 1>::try_new(
            FIRST,
            gateway(SECOND.instrument_id),
            command_consumer,
            event_producer,
        );
        assert!(matches!(
            result,
            Err(ShardConfigError::InstrumentMismatch {
                route: InstrumentId(11),
                gateway: InstrumentId(22),
            })
        ));
    }
}
