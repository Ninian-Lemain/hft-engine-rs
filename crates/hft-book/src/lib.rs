#![forbid(unsafe_code)]

use core::num::NonZeroUsize;

use hft_types::{
    AccountId, CancelOrder, ExecutionReport, InstrumentId, MatchSummary, NewOrder, OrderId,
    OrderState, PriceTicks, Quantity, RejectReason, ReplaceOrder, ReportBuffer, SequenceNumber,
    Side, TimeInForce,
};

// Price belongs to the containing level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestingOrder {
    id: OrderId,
    account_id: AccountId,
    index_slot: u32,
    quantity: Quantity,
    sequence: SequenceNumber,
    prev: usize,
    next: usize,
}

const NIL: usize = usize::MAX;

/// Fixed-capacity slot: a live FIFO node or a free-list link. Live and free
/// sets are disjoint by construction, and a slot handle stays stable while
/// unrelated slots mutate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrderSlot {
    Free { next_free: usize },
    Live(RestingOrder),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PriceLevel<const ORDERS: usize> {
    price: PriceTicks,
    // At most ORDERS u64 quantities fit in the addressable slot array.
    aggregate_quantity: u128,
    slots: [OrderSlot; ORDERS],
    head: usize,
    tail: usize,
    free_head: usize,
    len: usize,
}

const ORDER_INDEX_PLANES: usize = 4;

/// `slot` is a stable per-level slot handle, never a FIFO position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderLocation {
    side: Side,
    level_index: usize,
    slot: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexSlot {
    order_id: OrderId,
    location: Option<NonZeroUsize>,
}

impl IndexSlot {
    const EMPTY: Self = Self {
        order_id: OrderId(0),
        location: None,
    };
}

/// Open-addressed `OrderId -> OrderLocation` index with linear probing and
/// deterministic back-shift deletion. Both sides share one index: four planes
/// keep the live load at or below 50% of slots (two sides of at most
/// `LEVELS * ORDERS` orders each), so probes stay short at maximum occupancy.
/// The planes exist as a nested array because stable Rust cannot express
/// `LEVELS * ORDERS * PLANES` as a single array length over const generics.
#[derive(Debug)]
struct OrderIndex<const LEVELS: usize, const ORDERS: usize> {
    slots: [[[IndexSlot; ORDERS]; LEVELS]; ORDER_INDEX_PLANES],
}

impl<const LEVELS: usize, const ORDERS: usize> OrderIndex<LEVELS, ORDERS> {
    const CAPACITY: usize = LEVELS * ORDERS * ORDER_INDEX_PLANES;

    const fn new() -> Self {
        Self {
            slots: [[[IndexSlot::EMPTY; ORDERS]; LEVELS]; ORDER_INDEX_PLANES],
        }
    }

    fn encode_location(location: OrderLocation) -> Option<NonZeroUsize> {
        if location.level_index >= LEVELS || location.slot >= ORDERS {
            return None;
        }
        let flat = location
            .level_index
            .checked_mul(ORDERS)?
            .checked_add(location.slot)?;
        // The low bit selects the side. Adding one reserves zero for empty slots.
        let encoded = flat
            .checked_mul(2)?
            .checked_add(usize::from(location.side == Side::Sell))?
            .checked_add(1)?;
        NonZeroUsize::new(encoded)
    }

    fn decode_location(encoded: NonZeroUsize) -> Option<OrderLocation> {
        let encoded = encoded.get() - 1;
        let flat = encoded / 2;
        let level_index = flat.checked_div(ORDERS)?;
        if level_index >= LEVELS {
            return None;
        }
        Some(OrderLocation {
            side: if encoded & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            },
            level_index,
            slot: flat % ORDERS,
        })
    }

    /// Flat-index coordinates. Callers only pass indices below `CAPACITY`.
    fn coordinates(flat_index: usize) -> (usize, usize, usize) {
        debug_assert!(flat_index < Self::CAPACITY);
        let per_plane = LEVELS * ORDERS;
        let plane = flat_index / per_plane;
        let within_plane = flat_index % per_plane;
        (plane, within_plane / ORDERS, within_plane % ORDERS)
    }

    fn slot(&self, flat_index: usize) -> &IndexSlot {
        let (plane, level, order) = Self::coordinates(flat_index);
        &self.slots[plane][level][order]
    }

    fn slot_mut(&mut self, flat_index: usize) -> &mut IndexSlot {
        let (plane, level, order) = Self::coordinates(flat_index);
        &mut self.slots[plane][level][order]
    }

    /// Probing requires a non-zero capacity; callers guard `CAPACITY == 0`.
    fn probe_start(order_id: OrderId) -> usize {
        let capacity = u64::try_from(Self::CAPACITY).unwrap_or(u64::MAX);
        let mixed = order_id.0.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        usize::try_from(mixed % capacity).unwrap_or(0)
    }

    fn probe_distance(home: usize, current: usize) -> usize {
        if current >= home {
            current - home
        } else {
            Self::CAPACITY - (home - current)
        }
    }

    fn find_slot(&self, order_id: OrderId) -> Option<usize> {
        if Self::CAPACITY == 0 {
            return None;
        }
        let start = Self::probe_start(order_id);
        for offset in 0..Self::CAPACITY {
            let flat_index = start.wrapping_add(offset) % Self::CAPACITY;
            let slot = self.slot(flat_index);
            slot.location?;
            if slot.order_id == order_id {
                return Some(flat_index);
            }
        }
        None
    }

    fn location(&self, order_id: OrderId) -> Option<OrderLocation> {
        Self::decode_location(self.slot(self.find_slot(order_id)?).location?)
    }

    fn insert(&mut self, order_id: OrderId, location: OrderLocation) -> Result<u32, RejectReason> {
        if Self::CAPACITY == 0 {
            return Err(RejectReason::OrderCapacity);
        }
        let encoded = Self::encode_location(location).ok_or(RejectReason::OrderCapacity)?;
        let start = Self::probe_start(order_id);
        for offset in 0..Self::CAPACITY {
            let flat_index = start.wrapping_add(offset) % Self::CAPACITY;
            let slot = self.slot(flat_index);
            if slot.location.is_none() {
                let slot_id = u32::try_from(flat_index).map_err(|_| RejectReason::OrderCapacity)?;
                *self.slot_mut(flat_index) = IndexSlot {
                    order_id,
                    location: Some(encoded),
                };
                return Ok(slot_id);
            }
            if slot.order_id == order_id {
                return Err(RejectReason::DuplicateOrderId);
            }
        }
        Err(RejectReason::OrderCapacity)
    }

    /// Removes `order_id` at `slot_index` and closes the probe hole by
    /// shifting displaced entries back, reporting each move through
    /// `update_reverse_slot`. Fails closed on a stale handle.
    fn remove_at(
        &mut self,
        slot_index: u32,
        order_id: OrderId,
        mut update_reverse_slot: impl FnMut(OrderId, OrderLocation, u32) -> bool,
    ) -> Option<OrderLocation> {
        let flat_index = usize::try_from(slot_index).ok()?;
        if flat_index >= Self::CAPACITY {
            return None;
        }
        let indexed = *self.slot(flat_index);
        let location = Self::decode_location(indexed.location?)?;
        if indexed.order_id != order_id {
            return None;
        }
        let mut hole = flat_index;
        let mut candidate_index = hole.wrapping_add(1) % Self::CAPACITY;
        // Close the probe hole without retaining deletion tombstones.
        loop {
            let candidate = *self.slot(candidate_index);
            let Some(encoded) = candidate.location else {
                *self.slot_mut(hole) = IndexSlot::EMPTY;
                break;
            };
            let home_bucket = Self::probe_start(candidate.order_id);
            if Self::probe_distance(home_bucket, hole)
                < Self::probe_distance(home_bucket, candidate_index)
            {
                let candidate_location = Self::decode_location(encoded)?;
                *self.slot_mut(hole) = candidate;
                let new_slot = u32::try_from(hole).expect("occupied slots fit u32");
                // The closure must run in every profile; skipping it in
                // release strands the moved order's stored handle.
                let updated = update_reverse_slot(candidate.order_id, candidate_location, new_slot);
                debug_assert!(updated);
                hole = candidate_index;
            }
            candidate_index = candidate_index.wrapping_add(1) % Self::CAPACITY;
        }
        Some(location)
    }
}

impl<const ORDERS: usize> PriceLevel<ORDERS> {
    fn new(price: PriceTicks) -> Self {
        Self {
            price,
            aggregate_quantity: 0,
            slots: core::array::from_fn(|index| OrderSlot::Free {
                next_free: if index + 1 < ORDERS { index + 1 } else { NIL },
            }),
            head: NIL,
            tail: NIL,
            free_head: if ORDERS == 0 { NIL } else { 0 },
            len: 0,
        }
    }

    /// Appends at the tail and returns the stable slot handle, or `None` when
    /// the level is full or its links are corrupt. Nothing mutates on `None`.
    fn push_tail(&mut self, mut order: RestingOrder) -> Option<usize> {
        if self.free_head == NIL || self.len >= ORDERS {
            return None;
        }
        let slot = self.free_head;
        let Some(OrderSlot::Free { next_free }) = self.slots.get(slot).copied() else {
            return None;
        };
        if self.tail != NIL {
            match self.slots.get_mut(self.tail) {
                Some(OrderSlot::Live(tail_order)) => tail_order.next = slot,
                _ => return None,
            }
        }
        order.prev = self.tail;
        order.next = NIL;
        self.slots[slot] = OrderSlot::Live(order);
        self.free_head = next_free;
        if self.tail == NIL {
            self.head = slot;
        }
        self.tail = slot;
        self.len += 1;
        self.aggregate_quantity += u128::from(order.quantity.0);
        Some(slot)
    }

    /// Detaches a live slot and returns it to the free list. Stale or free
    /// handles fail closed without mutating the level.
    fn unlink(&mut self, slot: usize) -> Option<RestingOrder> {
        let Some(OrderSlot::Live(order)) = self.slots.get(slot).copied() else {
            return None;
        };
        if order.prev != NIL && !matches!(self.slots.get(order.prev), Some(OrderSlot::Live(_))) {
            return None;
        }
        if order.next != NIL && !matches!(self.slots.get(order.next), Some(OrderSlot::Live(_))) {
            return None;
        }
        match order.prev {
            NIL => self.head = order.next,
            prev => {
                if let Some(OrderSlot::Live(prev_order)) = self.slots.get_mut(prev) {
                    prev_order.next = order.next;
                }
            }
        }
        match order.next {
            NIL => self.tail = order.prev,
            next => {
                if let Some(OrderSlot::Live(next_order)) = self.slots.get_mut(next) {
                    next_order.prev = order.prev;
                }
            }
        }
        self.slots[slot] = OrderSlot::Free {
            next_free: self.free_head,
        };
        self.free_head = slot;
        self.len -= 1;
        self.aggregate_quantity -= u128::from(order.quantity.0);
        Some(order)
    }

    fn reduce_quantity(&mut self, slot: usize, quantity: Quantity) -> Option<()> {
        let order = self.get_live_mut(slot)?;
        order.quantity.0 = order.quantity.0.checked_sub(quantity.0)?;
        self.aggregate_quantity -= u128::from(quantity.0);
        Some(())
    }

    fn get_live(&self, slot: usize) -> Option<&RestingOrder> {
        match self.slots.get(slot) {
            Some(OrderSlot::Live(order)) => Some(order),
            _ => None,
        }
    }

    fn get_live_mut(&mut self, slot: usize) -> Option<&mut RestingOrder> {
        match self.slots.get_mut(slot) {
            Some(OrderSlot::Live(order)) => Some(order),
            _ => None,
        }
    }

    fn for_each_live(&self, mut visit: impl FnMut(&RestingOrder)) {
        let mut cursor = self.head;
        while cursor != NIL {
            let Some(OrderSlot::Live(order)) = self.slots.get(cursor) else {
                break;
            };
            visit(order);
            cursor = order.next;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelledOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub quantity: Quantity,
}

/// Outcome of an owned replace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplacedOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub old_quantity: Quantity,
    pub new_quantity: Quantity,
    pub price: PriceTicks,
    /// False only for same-price quantity reductions, which keep priority.
    pub priority_lost: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopLevel {
    pub price: PriceTicks,
    pub aggregate_quantity: u128,
    pub order_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookOrderState {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: Quantity,
    pub sequence: SequenceNumber,
}

const EMPTY_BOOK_ORDER_STATE: BookOrderState = BookOrderState {
    order_id: OrderId(0),
    account_id: AccountId(0),
    side: Side::Buy,
    price: PriceTicks(0),
    quantity: Quantity(0),
    sequence: SequenceNumber(0),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookLevelState<const ORDERS: usize> {
    pub side: Side,
    pub price: PriceTicks,
    pub order_count: usize,
    pub orders: [BookOrderState; ORDERS],
}

impl<const ORDERS: usize> BookLevelState<ORDERS> {
    #[must_use]
    pub const fn empty(side: Side, price: PriceTicks) -> Self {
        Self {
            side,
            price,
            order_count: 0,
            orders: [EMPTY_BOOK_ORDER_STATE; ORDERS],
        }
    }
}

const fn empty_book_level_state<const ORDERS: usize>() -> BookLevelState<ORDERS> {
    BookLevelState {
        side: Side::Buy,
        price: PriceTicks(0),
        order_count: 0,
        orders: [EMPTY_BOOK_ORDER_STATE; ORDERS],
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookState<const LEVELS: usize, const ORDERS: usize> {
    pub instrument: InstrumentId,
    pub bid_level_count: usize,
    pub bids: [BookLevelState<ORDERS>; LEVELS],
    pub ask_level_count: usize,
    pub asks: [BookLevelState<ORDERS>; LEVELS],
}

impl<const LEVELS: usize, const ORDERS: usize> BookState<LEVELS, ORDERS> {
    #[must_use]
    pub const fn empty(instrument: InstrumentId) -> Self {
        Self {
            instrument,
            bid_level_count: 0,
            bids: [empty_book_level_state(); LEVELS],
            ask_level_count: 0,
            asks: [empty_book_level_state(); LEVELS],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookStateError {
    Instrument,
    LevelCapacity,
    OrderCapacity,
    Side,
    Price,
    Quantity,
    Sequence,
    LevelOrder,
    DuplicateOrderId,
    Crossed,
    NonCanonical,
}

/// Sorted index of occupied level slots plus the pool of free level slots for
/// one side. Bids sort by price descending (best bid first); asks sort
/// ascending (best ask first). Price lookup is O(log n) by binary search,
/// best-price discovery is the first entry, and slot allocation is O(1) from
/// the free pool. Occupied and free slots partition `0..LEVELS` exactly.
#[derive(Debug)]
struct LevelIndex<const LEVELS: usize> {
    len: usize,
    entries: [(PriceTicks, usize); LEVELS],
    free: [usize; LEVELS],
    free_len: usize,
}

impl<const LEVELS: usize> LevelIndex<LEVELS> {
    const fn new() -> Self {
        let mut free = [0_usize; LEVELS];
        let mut index = 0;
        while index < LEVELS {
            // The pool pops from the end, so a fresh book allocates the lowest
            // level slots first.
            free[index] = LEVELS - 1 - index;
            index += 1;
        }
        Self {
            len: 0,
            entries: [(PriceTicks(0), 0); LEVELS],
            free,
            free_len: LEVELS,
        }
    }

    fn is_full(&self) -> bool {
        self.free_len == 0
    }

    /// Sorted position of `price`: the insertion point and whether an entry
    /// with that exact price occupies it.
    fn position(&self, price: PriceTicks, descending: bool) -> (usize, bool) {
        let entries = &self.entries[..self.len];
        let position = entries.partition_point(|&(listed, _)| {
            if descending {
                listed.0 > price.0
            } else {
                listed.0 < price.0
            }
        });
        let found = position < self.len && entries[position].0 == price;
        (position, found)
    }

    /// Level slot holding `price`, if one is occupied at that price.
    fn find(&self, price: PriceTicks, descending: bool) -> Option<usize> {
        let (position, found) = self.position(price, descending);
        if found {
            Some(self.entries[position].1)
        } else {
            None
        }
    }

    /// Allocates a free level slot and indexes it at `price` in sorted order.
    /// Returns the slot, or `None` when every level slot is occupied.
    fn insert(&mut self, price: PriceTicks, descending: bool) -> Option<usize> {
        let (position, found) = self.position(price, descending);
        debug_assert!(!found, "price levels are unique per side");
        if self.free_len == 0 {
            return None;
        }
        self.free_len -= 1;
        let level_index = self.free[self.free_len];
        self.entries.copy_within(position..self.len, position + 1);
        self.entries[position] = (price, level_index);
        self.len += 1;
        Some(level_index)
    }

    /// Removes the level at `price` and returns its slot to the free pool.
    fn remove(&mut self, price: PriceTicks, descending: bool) -> Option<usize> {
        let (position, found) = self.position(price, descending);
        if !found {
            return None;
        }
        let level_index = self.entries[position].1;
        self.entries.copy_within(position + 1..self.len, position);
        self.len -= 1;
        self.free[self.free_len] = level_index;
        self.free_len += 1;
        Some(level_index)
    }

    /// Walk entries in sorted order (best to worst for the given side).
    fn iter(&self) -> impl Iterator<Item = &(PriceTicks, usize)> {
        self.entries.iter().take(self.len)
    }
}

/// A single fill entry in a match plan. Records the maker order location and
/// the traded quantity so that `apply_plan` can execute the fill without
/// re-walking the book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FillEntry {
    level_index: usize,
    slot: usize,
    order_id: OrderId,
    price: PriceTicks,
    quantity: Quantity,
}

/// Compact match plan with bounded fill entries. Built during a single walk
/// of the crossing levels; applied by direct mutation using the captured
/// locations. A plan never heap-allocates and is bounded by the report
/// capacity available at build time.
#[derive(Clone, Debug)]
struct MatchPlan<const FILLS: usize> {
    fills: [FillEntry; FILLS],
    fill_count: usize,
    capacity: usize,
    maker_side: Side,
    resting_quantity: Quantity,
    /// IOC remainder that crossed nothing and is discarded, never filled.
    discarded: u64,
}

const DUMMY_FILL: FillEntry = FillEntry {
    level_index: 0,
    slot: 0,
    order_id: OrderId(0),
    price: PriceTicks(0),
    quantity: Quantity(0),
};

const DUMMY_PLAN_DISCARDED: u64 = 0;

impl<const FILLS: usize> MatchPlan<FILLS> {
    fn new(maker_side: Side, report_capacity: usize) -> Self {
        Self {
            fills: [DUMMY_FILL; FILLS],
            fill_count: 0,
            capacity: report_capacity.min(FILLS),
            maker_side,
            resting_quantity: Quantity(0),
            discarded: DUMMY_PLAN_DISCARDED,
        }
    }

    fn push_fill(
        &mut self,
        level_index: usize,
        slot: usize,
        order_id: OrderId,
        price: PriceTicks,
        quantity: Quantity,
    ) -> Result<(), RejectReason> {
        if self.fill_count >= self.capacity {
            return Err(RejectReason::ReportCapacity);
        }
        self.fills[self.fill_count] = FillEntry {
            level_index,
            slot,
            order_id,
            price,
            quantity,
        };
        self.fill_count += 1;
        Ok(())
    }

    #[must_use]
    fn fills(&self) -> &[FillEntry] {
        &self.fills[..self.fill_count]
    }
}

/// Single-instrument, fixed-capacity price-time book with indexed order
/// lookup and sorted-level best-price discovery. Per-level FIFOs use stable
/// slot handles: insert, fill, and cancel never shift peer orders or rewrite
/// their index locations.
#[derive(Debug)]
pub struct OrderBook<const LEVELS: usize, const ORDERS_PER_LEVEL: usize> {
    instrument: InstrumentId,
    bids: [Option<PriceLevel<ORDERS_PER_LEVEL>>; LEVELS],
    asks: [Option<PriceLevel<ORDERS_PER_LEVEL>>; LEVELS],
    index: OrderIndex<LEVELS, ORDERS_PER_LEVEL>,
    bid_levels: LevelIndex<LEVELS>,
    ask_levels: LevelIndex<LEVELS>,
}

impl<const LEVELS: usize, const ORDERS_PER_LEVEL: usize> OrderBook<LEVELS, ORDERS_PER_LEVEL> {
    #[must_use]
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the best level and its maintained quantity total.
    #[must_use]
    pub fn top_level(&self, side: Side) -> Option<TopLevel> {
        let &(price, level_index) = self.side_index(side).iter().next()?;
        let level = self.side_levels(side).get(level_index)?.as_ref()?;
        Some(TopLevel {
            price,
            aggregate_quantity: level.aggregate_quantity,
            order_count: level.len,
        })
    }

    #[must_use]
    pub const fn new(instrument: InstrumentId) -> Self {
        Self {
            instrument,
            bids: [None; LEVELS],
            asks: [None; LEVELS],
            index: OrderIndex::new(),
            bid_levels: LevelIndex::new(),
            ask_levels: LevelIndex::new(),
        }
    }

    #[must_use]
    pub fn export_state(&self) -> BookState<LEVELS, ORDERS_PER_LEVEL> {
        let mut state = BookState::empty(self.instrument);
        self.export_side(Side::Buy, &mut state.bids, &mut state.bid_level_count);
        self.export_side(Side::Sell, &mut state.asks, &mut state.ask_level_count);
        state
    }

    fn export_side(
        &self,
        side: Side,
        output: &mut [BookLevelState<ORDERS_PER_LEVEL>; LEVELS],
        output_len: &mut usize,
    ) {
        for &(price, level_index) in self.side_index(side).iter() {
            let Some(level) = self.side_levels(side)[level_index].as_ref() else {
                continue;
            };
            let destination = &mut output[*output_len];
            destination.side = side;
            destination.price = price;
            level.for_each_live(|order| {
                destination.orders[destination.order_count] = BookOrderState {
                    order_id: order.id,
                    account_id: order.account_id,
                    side,
                    price,
                    quantity: order.quantity,
                    sequence: order.sequence,
                };
                destination.order_count += 1;
            });
            *output_len += 1;
        }
    }

    /// Builds a book from canonical logical state.
    ///
    /// # Errors
    ///
    /// Returns the first format, capacity, ordering, identity, or book
    /// invariant failure. The returned book is either complete or absent.
    pub fn from_state(
        instrument: InstrumentId,
        state: &BookState<LEVELS, ORDERS_PER_LEVEL>,
    ) -> Result<Self, BookStateError> {
        if state.instrument != instrument {
            return Err(BookStateError::Instrument);
        }
        Self::validate_state(state)?;
        let mut book = Self::new(instrument);
        for level in state.bids.iter().take(state.bid_level_count) {
            book.restore_level(level)?;
        }
        for level in state.asks.iter().take(state.ask_level_count) {
            book.restore_level(level)?;
        }
        Ok(book)
    }

    fn validate_state(state: &BookState<LEVELS, ORDERS_PER_LEVEL>) -> Result<(), BookStateError> {
        if state.bid_level_count > LEVELS || state.ask_level_count > LEVELS {
            return Err(BookStateError::LevelCapacity);
        }
        Self::validate_side(&state.bids, state.bid_level_count, Side::Buy)?;
        Self::validate_side(&state.asks, state.ask_level_count, Side::Sell)?;
        if state.bids[state.bid_level_count..]
            .iter()
            .chain(&state.asks[state.ask_level_count..])
            .any(|level| *level != empty_book_level_state())
        {
            return Err(BookStateError::NonCanonical);
        }
        if state
            .bids
            .first()
            .filter(|_| state.bid_level_count > 0)
            .zip(state.asks.first().filter(|_| state.ask_level_count > 0))
            .is_some_and(|(bid, ask)| bid.price.0 >= ask.price.0)
        {
            return Err(BookStateError::Crossed);
        }
        Self::validate_unique_ids(state)
    }

    fn validate_unique_ids(
        state: &BookState<LEVELS, ORDERS_PER_LEVEL>,
    ) -> Result<(), BookStateError> {
        for (levels, level_count) in [
            (&state.bids, state.bid_level_count),
            (&state.asks, state.ask_level_count),
        ] {
            for level in levels.iter().take(level_count) {
                for order in level.orders.iter().take(level.order_count) {
                    let mut occurrences = 0_usize;
                    for (candidate_levels, candidate_count) in [
                        (&state.bids, state.bid_level_count),
                        (&state.asks, state.ask_level_count),
                    ] {
                        for candidate_level in candidate_levels.iter().take(candidate_count) {
                            occurrences += candidate_level
                                .orders
                                .iter()
                                .take(candidate_level.order_count)
                                .filter(|candidate| candidate.order_id == order.order_id)
                                .count();
                        }
                    }
                    if occurrences != 1 {
                        return Err(BookStateError::DuplicateOrderId);
                    }
                    let sequence_occurrences = state
                        .bids
                        .iter()
                        .take(state.bid_level_count)
                        .chain(state.asks.iter().take(state.ask_level_count))
                        .flat_map(|candidate_level| {
                            candidate_level
                                .orders
                                .iter()
                                .take(candidate_level.order_count)
                        })
                        .filter(|candidate| candidate.sequence == order.sequence)
                        .count();
                    if sequence_occurrences != 1 {
                        return Err(BookStateError::Sequence);
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_side(
        levels: &[BookLevelState<ORDERS_PER_LEVEL>; LEVELS],
        level_count: usize,
        side: Side,
    ) -> Result<(), BookStateError> {
        let mut prior_price = None;
        for level in levels.iter().take(level_count) {
            if level.side != side {
                return Err(BookStateError::Side);
            }
            if level.price.0 <= 0 {
                return Err(BookStateError::Price);
            }
            if level.order_count == 0 || level.order_count > ORDERS_PER_LEVEL {
                return Err(BookStateError::OrderCapacity);
            }
            if prior_price.is_some_and(|prior: PriceTicks| match side {
                Side::Buy => prior.0 <= level.price.0,
                Side::Sell => prior.0 >= level.price.0,
            }) {
                return Err(BookStateError::LevelOrder);
            }
            for order in level.orders.iter().take(level.order_count) {
                if order.side != side {
                    return Err(BookStateError::Side);
                }
                if order.price != level.price {
                    return Err(BookStateError::Price);
                }
                if order.quantity.0 == 0 {
                    return Err(BookStateError::Quantity);
                }
                if order.sequence.0 == 0 {
                    return Err(BookStateError::Sequence);
                }
            }
            if level.orders[..level.order_count]
                .windows(2)
                .any(|pair| pair[0].sequence >= pair[1].sequence)
            {
                return Err(BookStateError::Sequence);
            }
            if level.orders[level.order_count..]
                .iter()
                .any(|order| *order != EMPTY_BOOK_ORDER_STATE)
            {
                return Err(BookStateError::NonCanonical);
            }
            prior_price = Some(level.price);
        }
        Ok(())
    }

    fn restore_level(
        &mut self,
        state: &BookLevelState<ORDERS_PER_LEVEL>,
    ) -> Result<(), BookStateError> {
        let descending = state.side == Side::Buy;
        let level_index = self
            .side_index_mut(state.side)
            .insert(state.price, descending)
            .ok_or(BookStateError::LevelCapacity)?;
        self.side_levels_mut(state.side)[level_index] = Some(PriceLevel::new(state.price));
        for order in state.orders.iter().take(state.order_count) {
            let resting = RestingOrder {
                id: order.order_id,
                account_id: order.account_id,
                index_slot: 0,
                quantity: order.quantity,
                sequence: order.sequence,
                prev: NIL,
                next: NIL,
            };
            let slot = self.side_levels_mut(state.side)[level_index]
                .as_mut()
                .and_then(|level| level.push_tail(resting))
                .ok_or(BookStateError::OrderCapacity)?;
            let location = OrderLocation {
                side: state.side,
                level_index,
                slot,
            };
            let index_slot = self
                .index
                .insert(order.order_id, location)
                .map_err(|_| BookStateError::DuplicateOrderId)?;
            let live = self.side_levels_mut(state.side)[level_index]
                .as_mut()
                .and_then(|level| level.get_live_mut(slot))
                .ok_or(BookStateError::OrderCapacity)?;
            live.index_slot = index_slot;
        }
        Ok(())
    }

    fn side_levels(&self, side: Side) -> &[Option<PriceLevel<ORDERS_PER_LEVEL>>; LEVELS] {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_levels_mut(
        &mut self,
        side: Side,
    ) -> &mut [Option<PriceLevel<ORDERS_PER_LEVEL>>; LEVELS] {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn side_index(&self, side: Side) -> &LevelIndex<LEVELS> {
        match side {
            Side::Buy => &self.bid_levels,
            Side::Sell => &self.ask_levels,
        }
    }

    fn side_index_mut(&mut self, side: Side) -> &mut LevelIndex<LEVELS> {
        match side {
            Side::Buy => &mut self.bid_levels,
            Side::Sell => &mut self.ask_levels,
        }
    }

    /// # Errors
    ///
    /// Returns an explicit validation or capacity rejection. `build_plan`
    /// preflights every fallible condition, so a rejection provably leaves the
    /// book and report buffer untouched.
    pub fn submit<const REPORTS: usize>(
        &mut self,
        order: NewOrder,
        reports: &mut ReportBuffer<REPORTS>,
    ) -> Result<MatchSummary, RejectReason> {
        let initial_report_count = reports.len();
        let plan = self.build_plan::<REPORTS>(order, reports.remaining_capacity())?;
        self.apply_plan(order, &plan, reports);
        let filled = order.quantity.0 - plan.resting_quantity.0 - plan.discarded;
        let state = if filled == order.quantity.0 {
            OrderState::Filled
        } else if filled == 0 {
            OrderState::Accepted
        } else {
            OrderState::PartiallyFilled
        };
        Ok(MatchSummary {
            state,
            filled_quantity: Quantity(filled),
            resting_quantity: plan.resting_quantity,
            discarded_quantity: Quantity(plan.discarded),
            report_count: reports.len() - initial_report_count,
        })
    }

    /// Walks crossing levels once to build a compact match plan, preflighting
    /// validation, duplicates, report capacity, and resting capacity. Because
    /// every fallible condition is decided here against an unchanged book,
    /// `apply_plan` afterwards is infallible and needs no rollback path.
    fn build_plan<const REPORTS: usize>(
        &self,
        order: NewOrder,
        report_capacity: usize,
    ) -> Result<MatchPlan<REPORTS>, RejectReason> {
        if order.instrument_id != self.instrument {
            return Err(RejectReason::InvalidInstrument);
        }
        if order.price.0 <= 0 {
            return Err(RejectReason::InvalidPrice);
        }
        if order.quantity.0 == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if self.contains_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let maker_side = match order.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        if order.time_in_force == TimeInForce::PostOnly {
            // Best-price check: the sorted index puts the best level first,
            // so if it does not cross, nothing does.
            if let Some(&(best_price, _)) = self.side_index(maker_side).iter().next() {
                let crosses = match order.side {
                    Side::Buy => best_price.0 <= order.price.0,
                    Side::Sell => best_price.0 >= order.price.0,
                };
                if crosses {
                    return Err(RejectReason::PostOnlyWouldTrade);
                }
            }
        }
        let maker_levels = self.side_levels(maker_side);
        let mut plan = MatchPlan::<REPORTS>::new(maker_side, report_capacity);
        let mut remaining = order.quantity.0;
        let mut discarded = 0_u64;
        for &(price, level_index) in self.side_index(maker_side).iter() {
            if remaining == 0 {
                break;
            }
            let crosses = match order.side {
                Side::Buy => price.0 <= order.price.0,
                Side::Sell => price.0 >= order.price.0,
            };
            // Levels are visited best to worst, so the first non-crossing
            // level ends the walk.
            if !crosses {
                break;
            }
            let level = maker_levels[level_index]
                .as_ref()
                .expect("indexed level is occupied");
            let mut cursor = level.head;
            while cursor != NIL && remaining > 0 {
                let OrderSlot::Live(maker) = level.slots[cursor] else {
                    unreachable!("live chain only links live slots");
                };
                let traded = remaining.min(maker.quantity.0);
                plan.push_fill(level_index, cursor, maker.id, price, Quantity(traded))?;
                remaining -= traded;
                cursor = maker.next;
            }
        }
        match order.time_in_force {
            TimeInForce::Fok => {
                // Fill everything within the price limit or touch nothing.
                if remaining > 0 {
                    return Err(RejectReason::InsufficientLiquidity);
                }
            }
            TimeInForce::Ioc => {
                // Execute what crossed and discard the remainder.
                discarded = remaining;
                remaining = 0;
            }
            TimeInForce::Gtc | TimeInForce::PostOnly => {
                if remaining > 0 {
                    let descending = order.side == Side::Buy;
                    if let Some(level_index) =
                        self.side_index(order.side).find(order.price, descending)
                    {
                        let level = self.side_levels(order.side)[level_index]
                            .as_ref()
                            .expect("indexed level is occupied");
                        if level.len == ORDERS_PER_LEVEL {
                            return Err(RejectReason::PriceLevelOrderCapacity);
                        }
                    } else if self.side_index(order.side).is_full() {
                        return Err(RejectReason::PriceLevelCapacity);
                    }
                }
            }
        }
        plan.resting_quantity = Quantity(remaining);
        plan.discarded = discarded;
        Ok(plan)
    }

    /// Applies a pre-built match plan, emitting one execution report per fill.
    /// Infallible: `build_plan` preflighted report capacity, level capacity,
    /// and duplicates, and the book cannot change between the two calls.
    /// Violations of those preflighted invariants are bugs, not rejections.
    fn apply_plan<const REPORTS: usize>(
        &mut self,
        order: NewOrder,
        plan: &MatchPlan<REPORTS>,
        reports: &mut ReportBuffer<REPORTS>,
    ) {
        for fill in plan.fills() {
            let (index_slot, full_fill) = {
                let level = self.side_levels(plan.maker_side)[fill.level_index]
                    .as_ref()
                    .expect("plan level is occupied");
                let maker = level.get_live(fill.slot).expect("plan maker is live");
                debug_assert_eq!(maker.id, fill.order_id);
                reports
                    .push(ExecutionReport {
                        maker_order_id: fill.order_id,
                        taker_order_id: order.order_id,
                        instrument_id: order.instrument_id,
                        price: fill.price,
                        quantity: fill.quantity,
                        sequence: order.sequence,
                    })
                    .expect("report capacity was preflighted");
                (maker.index_slot, maker.quantity.0 == fill.quantity.0)
            };
            if full_fill {
                let removed_location = self.remove_index_entry(index_slot, fill.order_id);
                debug_assert_eq!(
                    removed_location,
                    Some(OrderLocation {
                        side: plan.maker_side,
                        level_index: fill.level_index,
                        slot: fill.slot,
                    })
                );
                let level = self.side_levels_mut(plan.maker_side)[fill.level_index]
                    .as_mut()
                    .expect("plan level is occupied");
                let removed = level.unlink(fill.slot).expect("plan maker is live");
                debug_assert_eq!(removed.quantity, fill.quantity);
                if level.len == 0 {
                    let price = level.price;
                    self.side_levels_mut(plan.maker_side)[fill.level_index] = None;
                    let removed_index = self
                        .side_index_mut(plan.maker_side)
                        .remove(price, plan.maker_side == Side::Buy);
                    debug_assert_eq!(removed_index, Some(fill.level_index));
                }
            } else {
                self.side_levels_mut(plan.maker_side)[fill.level_index]
                    .as_mut()
                    .and_then(|level| level.reduce_quantity(fill.slot, fill.quantity))
                    .expect("plan quantity fits the live maker");
            }
        }
        if plan.resting_quantity.0 > 0 {
            self.rest(order, plan.resting_quantity);
        }
    }

    /// Amends an owned resting order. A pure quantity reduction keeps its
    /// slot and FIFO priority; any price change or quantity increase loses
    /// priority and re-enters at the destination level tail. Repricing never
    /// executes against the book, so a replace whose new price would cross
    /// the opposing best price is rejected before mutation.
    ///
    /// # Errors
    ///
    /// Returns instrument, unknown-order, ownership, validation, crossing,
    /// or capacity rejection without mutating the book.
    ///
    /// # Panics
    ///
    /// Panics only if internal index invariants are broken, which is a bug,
    /// not a rejection path.
    #[allow(clippy::too_many_lines)]
    pub fn replace(&mut self, replace: ReplaceOrder) -> Result<ReplacedOrder, RejectReason> {
        if replace.instrument_id != self.instrument {
            return Err(RejectReason::InvalidInstrument);
        }
        let (location, owner) = self
            .locate(replace.order_id)
            .ok_or(RejectReason::UnknownOrder)?;
        if owner != replace.account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        if replace.price.0 <= 0 {
            return Err(RejectReason::InvalidPrice);
        }
        if replace.quantity.0 == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        let (old, old_price) = {
            let level = self.side_levels(location.side)[location.level_index]
                .as_ref()
                .expect("indexed level is occupied");
            (
                *level.get_live(location.slot).expect("indexed slot is live"),
                level.price,
            )
        };
        let priority_kept = replace.price == old_price && replace.quantity.0 < old.quantity.0;
        if !priority_kept {
            if replace.price != old_price {
                // A repriced order must not cross: check the opposing best.
                let opposing = match location.side {
                    Side::Buy => Side::Sell,
                    Side::Sell => Side::Buy,
                };
                if let Some(&(best_price, _)) = self.side_index(opposing).iter().next() {
                    let crosses = match location.side {
                        Side::Buy => replace.price.0 >= best_price.0,
                        Side::Sell => replace.price.0 <= best_price.0,
                    };
                    if crosses {
                        return Err(RejectReason::ReplaceWouldCross);
                    }
                }
            }
            // Destination capacity preflight against the unchanged book.
            let descending = location.side == Side::Buy;
            if let Some(level_index) = self
                .side_index(location.side)
                .find(replace.price, descending)
            {
                let dest_len = self.side_levels(location.side)[level_index]
                    .as_ref()
                    .expect("dest level")
                    .len;
                let same_level = level_index == location.level_index && replace.price == old_price;
                if dest_len == ORDERS_PER_LEVEL && !same_level {
                    return Err(RejectReason::PriceLevelOrderCapacity);
                }
            } else {
                let source_empties = self.side_levels(location.side)[location.level_index]
                    .as_ref()
                    .expect("source level")
                    .len
                    == 1;
                let free_levels = LEVELS - self.side_index(location.side).iter().count();
                if free_levels == 0 && !source_empties {
                    return Err(RejectReason::PriceLevelCapacity);
                }
            }
        }

        if priority_kept {
            // In-place reduction: slot handle, index entry, and FIFO
            // position all stay untouched.
            let level = self.side_levels_mut(location.side)[location.level_index]
                .as_mut()
                .expect("indexed level is occupied");
            level
                .reduce_quantity(location.slot, Quantity(old.quantity.0 - replace.quantity.0))
                .expect("replacement reduces the live order");
            return Ok(ReplacedOrder {
                order_id: replace.order_id,
                account_id: replace.account_id,
                old_quantity: Quantity(old.quantity.0),
                new_quantity: Quantity(replace.quantity.0),
                price: replace.price,
                priority_lost: false,
            });
        }

        // Priority lost: unlink, drop an emptied source level, re-add at the
        // destination tail.
        let removed = {
            let level = self.side_levels_mut(location.side)[location.level_index]
                .as_mut()
                .expect("indexed level is occupied");
            level.unlink(location.slot).expect("indexed slot is live")
        };
        debug_assert_eq!(removed.id, replace.order_id);
        let source_emptied = self.side_levels(location.side)[location.level_index]
            .as_ref()
            .expect("source level")
            .len
            == 0;
        if source_emptied {
            self.side_levels_mut(location.side)[location.level_index] = None;
            let removed_index = self
                .side_index_mut(location.side)
                .remove(old_price, location.side == Side::Buy);
            debug_assert_eq!(removed_index, Some(location.level_index));
        }
        let removed_location = self.remove_index_entry(removed.index_slot, replace.order_id);
        debug_assert_eq!(removed_location, Some(location));
        let resting = RestingOrder {
            id: replace.order_id,
            account_id: replace.account_id,
            index_slot: 0,
            quantity: Quantity(replace.quantity.0),
            sequence: replace.sequence,
            prev: NIL,
            next: NIL,
        };
        self.rest_at_tail(location.side, replace.price, resting);
        Ok(ReplacedOrder {
            order_id: replace.order_id,
            account_id: replace.account_id,
            old_quantity: Quantity(old.quantity.0),
            new_quantity: Quantity(replace.quantity.0),
            price: replace.price,
            priority_lost: true,
        })
    }
    /// Removes an owned resting order while retaining FIFO order for all peers.
    ///
    /// # Errors
    ///
    /// Returns an instrument, unknown-order, or ownership rejection without
    /// mutating the book.
    ///
    /// # Panics
    ///
    /// Panics only if the book's internal index invariants are broken, which
    /// is a bug, not a rejection path.
    pub fn cancel(&mut self, cancel: CancelOrder) -> Result<CancelledOrder, RejectReason> {
        if cancel.instrument_id != self.instrument {
            return Err(RejectReason::InvalidInstrument);
        }
        let (location, owner) = self
            .locate(cancel.order_id)
            .ok_or(RejectReason::UnknownOrder)?;
        if owner != cancel.account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        // Located and owner-checked: removal cannot fail.
        let level = self.side_levels_mut(location.side)[location.level_index]
            .as_mut()
            .expect("indexed level is occupied");
        let removed = level.unlink(location.slot).expect("indexed slot is live");
        debug_assert_eq!(removed.id, cancel.order_id);
        if level.len == 0 {
            let price = level.price;
            self.side_levels_mut(location.side)[location.level_index] = None;
            let removed_index = self
                .side_index_mut(location.side)
                .remove(price, location.side == Side::Buy);
            debug_assert_eq!(removed_index, Some(location.level_index));
        }
        let removed_location = self.remove_index_entry(removed.index_slot, cancel.order_id);
        debug_assert_eq!(removed_location, Some(location));
        Ok(CancelledOrder {
            order_id: removed.id,
            account_id: removed.account_id,
            quantity: removed.quantity,
        })
    }

    #[cfg(test)]
    fn best_crossing_level(&self, side: Side, price: PriceTicks) -> Option<usize> {
        let maker_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let &(level_price, level_index) = self.side_index(maker_side).iter().next()?;
        let crosses = match side {
            Side::Buy => level_price.0 <= price.0,
            Side::Sell => level_price.0 >= price.0,
        };
        if crosses { Some(level_index) } else { None }
    }

    /// Rests the unfilled remainder. Infallible: `build_plan` preflighted a
    /// level slot with room or a free level slot, and the order index always
    /// has spare capacity (four planes for at most `2 * LEVELS * ORDERS` live
    /// orders).
    fn rest(&mut self, order: NewOrder, quantity: Quantity) {
        let resting = RestingOrder {
            id: order.order_id,
            account_id: order.account_id,
            index_slot: 0,
            quantity,
            sequence: order.sequence,
            prev: NIL,
            next: NIL,
        };
        self.rest_at_tail(order.side, order.price, resting);
    }

    /// Appends one already-validated resting order at the side tail and
    /// indexes it. Infallible for preflighted callers.
    fn rest_at_tail(&mut self, side: Side, price: PriceTicks, resting: RestingOrder) {
        let Self {
            bids,
            asks,
            index,
            bid_levels,
            ask_levels,
            ..
        } = self;
        let (levels, sorted) = match side {
            Side::Buy => (bids, bid_levels),
            Side::Sell => (asks, ask_levels),
        };
        let descending = side == Side::Buy;
        let level_index = if let Some(level_index) = sorted.find(price, descending) {
            level_index
        } else {
            let level_index = sorted
                .insert(price, descending)
                .expect("level capacity was preflighted");
            levels[level_index] = Some(PriceLevel::new(price));
            level_index
        };
        let level = levels[level_index]
            .as_mut()
            .expect("indexed level is occupied");
        let slot = level
            .push_tail(resting)
            .expect("level order capacity was preflighted");
        let location = OrderLocation {
            side,
            level_index,
            slot,
        };
        let index_slot = index
            .insert(resting.id, location)
            .expect("order index capacity was preflighted");
        level
            .get_live_mut(slot)
            .expect("slot was just inserted")
            .index_slot = index_slot;
    }

    fn remove_index_entry(&mut self, index_slot: u32, order_id: OrderId) -> Option<OrderLocation> {
        let (bids, asks, index) = (&mut self.bids, &mut self.asks, &mut self.index);
        index.remove_at(index_slot, order_id, |moved_id, location, new_slot| {
            let levels = match location.side {
                Side::Buy => &mut *bids,
                Side::Sell => &mut *asks,
            };
            let Some(resting) = levels
                .get_mut(location.level_index)
                .and_then(Option::as_mut)
                .and_then(|level| level.get_live_mut(location.slot))
            else {
                return false;
            };
            if resting.id != moved_id {
                return false;
            }
            resting.index_slot = new_slot;
            true
        })
    }

    fn contains_order(&self, order_id: OrderId) -> bool {
        self.index.location(order_id).is_some()
    }

    /// Resolves an order ID to its location and owner, failing closed when the
    /// index entry is stale.
    fn locate(&self, order_id: OrderId) -> Option<(OrderLocation, AccountId)> {
        let location = self.index.location(order_id)?;
        let order = self
            .side_levels(location.side)
            .get(location.level_index)?
            .as_ref()?
            .get_live(location.slot)?;
        if order.id != order_id {
            return None;
        }
        Some((location, order.account_id))
    }

    #[must_use]
    pub fn stable_digest(&self) -> u64 {
        let mut digest = 0xcbf2_9ce4_8422_2325_u64;
        mix(&mut digest, u64::from(self.instrument.0));
        self.digest_side(&mut digest, Side::Buy);
        self.digest_side(&mut digest, Side::Sell);
        digest
    }

    fn digest_side(&self, digest: &mut u64, side: Side) {
        mix(digest, u64::from(side as u8));
        let levels = self.side_levels(side);
        for &(_, level_index) in self.side_index(side).iter() {
            if let Some(level) = levels[level_index].as_ref() {
                // Bit-identical on any CPU byte order (MSRV precludes
                // `cast_unsigned`).
                mix(digest, u64::from_be_bytes(level.price.0.to_be_bytes()));
                level.for_each_live(|order| {
                    mix(digest, order.id.0);
                    mix(digest, u64::from(order.account_id.0));
                    mix(digest, order.quantity.0);
                    mix(digest, order.sequence.0);
                });
            }
        }
    }

    #[must_use]
    pub fn order_count(&self) -> usize {
        self.bids
            .iter()
            .chain(&self.asks)
            .flatten()
            .map(|level| level.len)
            .sum()
    }
}

fn mix(digest: &mut u64, value: u64) {
    *digest ^= value;
    *digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_types::AccountId;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn resting_slots_fit_in_fifty_six_bytes() {
        assert_eq!(core::mem::size_of::<RestingOrder>(), 48);
        assert_eq!(core::mem::size_of::<OrderSlot>(), 56);
        assert_eq!(core::mem::size_of::<Option<PriceLevel<64>>>(), 3_648);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn order_index_slots_fit_in_sixteen_bytes() {
        assert_eq!(core::mem::size_of::<IndexSlot>(), 16);
    }

    #[test]
    fn compact_locations_cover_both_sides_and_reject_invalid_handles() {
        type Index = OrderIndex<3, 5>;
        for side in [Side::Buy, Side::Sell] {
            for level_index in 0..3 {
                for slot in 0..5 {
                    let location = OrderLocation {
                        side,
                        level_index,
                        slot,
                    };
                    let encoded = Index::encode_location(location).expect("valid location");
                    assert_eq!(Index::decode_location(encoded), Some(location));
                }
            }
        }
        let first = OrderLocation {
            side: Side::Buy,
            level_index: 0,
            slot: 0,
        };
        for invalid in [
            OrderLocation {
                level_index: 3,
                ..first
            },
            OrderLocation { slot: 5, ..first },
            OrderLocation {
                level_index: usize::MAX,
                ..first
            },
        ] {
            assert_eq!(Index::encode_location(invalid), None);
        }
        assert_eq!(
            Index::decode_location(NonZeroUsize::new(31).expect("nonzero")),
            None
        );
        assert_eq!(OrderIndex::<0, 5>::encode_location(first), None);
        assert_eq!(OrderIndex::<3, 0>::encode_location(first), None);
        assert_eq!(OrderIndex::<3, 0>::decode_location(NonZeroUsize::MIN), None);
    }

    #[test]
    fn compact_index_preserves_zero_and_maximum_order_ids() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<2>::new();
        for (id, side, price) in [(0, Side::Buy, 99), (u64::MAX, Side::Sell, 101)] {
            book.submit(order(id, price, 1, side), &mut reports)
                .expect("rest");
            assert!(book.contains_order(OrderId(id)));
            assert_index_consistent(&book);
        }
        for id in [0, u64::MAX] {
            book.cancel(CancelOrder {
                order_id: OrderId(id),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(1),
            })
            .expect("cancel");
            assert!(!book.contains_order(OrderId(id)));
            assert_index_consistent(&book);
        }
    }
    fn order(id: u64, price: i64, quantity: u64, side: Side) -> NewOrder {
        NewOrder {
            time_in_force: hft_types::TimeInForce::Gtc,
            order_id: OrderId(id),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            price: PriceTicks(price),
            quantity: Quantity(quantity),
            sequence: SequenceNumber(id),
            side,
        }
    }

    fn assert_index_consistent<const LEVELS: usize, const ORDERS: usize>(
        book: &OrderBook<LEVELS, ORDERS>,
    ) {
        let mut live_orders = 0_usize;
        for (side, levels) in [(Side::Buy, &book.bids), (Side::Sell, &book.asks)] {
            for (level_index, level) in levels.iter().enumerate() {
                let Some(level) = level else {
                    continue;
                };
                let mut seen = 0_usize;
                let mut aggregate_quantity = 0_u128;
                let mut prev = NIL;
                let mut cursor = level.head;
                while cursor != NIL {
                    let OrderSlot::Live(resting) =
                        level.slots.get(cursor).expect("chain slot in range")
                    else {
                        panic!("live chain reached a free slot");
                    };
                    assert_eq!(resting.prev, prev, "prev link matches walk");
                    let location = OrderLocation {
                        side,
                        level_index,
                        slot: cursor,
                    };
                    assert_eq!(book.index.location(resting.id), Some(location));
                    assert_eq!(
                        book.index
                            .slot(usize::try_from(resting.index_slot).expect("valid index slot")),
                        &IndexSlot {
                            order_id: resting.id,
                            location: OrderIndex::<LEVELS, ORDERS>::encode_location(location),
                        }
                    );
                    prev = cursor;
                    cursor = resting.next;
                    seen += 1;
                    aggregate_quantity += u128::from(resting.quantity.0);
                }
                assert_eq!(prev, level.tail, "tail matches walk end");
                assert_eq!(seen, level.len, "live count matches len");
                assert_eq!(aggregate_quantity, level.aggregate_quantity);
                let mut free_seen = 0_usize;
                let mut cursor = level.free_head;
                while cursor != NIL {
                    let OrderSlot::Free { next_free } =
                        level.slots.get(cursor).expect("free slot in range")
                    else {
                        panic!("free chain reached a live slot");
                    };
                    cursor = *next_free;
                    free_seen += 1;
                }
                assert_eq!(seen + free_seen, ORDERS, "live and free sets cover slots");
                live_orders += seen;
            }
        }
        let indexed_orders = book
            .index
            .slots
            .iter()
            .flatten()
            .flatten()
            .filter(|slot| slot.location.is_some())
            .count();
        assert_eq!(indexed_orders, live_orders);
        assert_eq!(book.order_count(), live_orders);
    }

    #[test]
    fn matches_price_then_time() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 101, 2, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        book.submit(order(2, 100, 2, Side::Sell), &mut reports)
            .expect("rest better ask");
        reports.clear();
        book.submit(order(3, 100, 2, Side::Sell), &mut reports)
            .expect("rest same ask");
        reports.clear();
        let summary = book
            .submit(order(4, 101, 5, Side::Buy), &mut reports)
            .expect("cross asks");
        let makers: std::vec::Vec<_> = reports.iter().map(|report| report.maker_order_id).collect();
        assert_eq!(makers, [OrderId(2), OrderId(3), OrderId(1)]);
        assert_eq!(summary.filled_quantity, Quantity(5));
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn top_level_reports_best_price_aggregate_and_order_count() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();

        assert_eq!(book.top_level(Side::Buy), None);
        assert_eq!(book.top_level(Side::Sell), None);

        book.submit(order(1, 99, 2, Side::Buy), &mut reports)
            .expect("rest lower bid");
        reports.clear();
        book.submit(order(2, 100, 3, Side::Buy), &mut reports)
            .expect("rest best bid");
        reports.clear();
        book.submit(order(3, 100, 4, Side::Buy), &mut reports)
            .expect("rest at best bid");
        reports.clear();
        book.submit(order(4, 102, 5, Side::Sell), &mut reports)
            .expect("rest ask");

        assert_eq!(
            book.top_level(Side::Buy),
            Some(TopLevel {
                price: PriceTicks(100),
                aggregate_quantity: 7,
                order_count: 2,
            })
        );
        assert_eq!(
            book.top_level(Side::Sell),
            Some(TopLevel {
                price: PriceTicks(102),
                aggregate_quantity: 5,
                order_count: 1,
            })
        );

        reports.clear();
        book.submit(order(5, 100, 5, Side::Sell), &mut reports)
            .expect("reduce best bid");
        assert_eq!(
            book.top_level(Side::Buy),
            Some(TopLevel {
                price: PriceTicks(100),
                aggregate_quantity: 2,
                order_count: 1,
            })
        );
    }

    #[test]
    fn level_totals_survive_large_quantities_replace_and_restore() {
        for side in [Side::Buy, Side::Sell] {
            let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
            let mut reports = ReportBuffer::<2>::new();
            for id in 1..=2 {
                book.submit(order(id, 100, u64::MAX, side), &mut reports)
                    .expect("rest large order");
            }
            assert_eq!(
                book.top_level(side).expect("level").aggregate_quantity,
                2 * u128::from(u64::MAX)
            );
            let opposing = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            book.submit(order(3, 100, 10, opposing), &mut reports)
                .expect("partial fill");
            assert_eq!(
                book.top_level(side).expect("level").aggregate_quantity,
                2 * u128::from(u64::MAX) - 10
            );
            assert_index_consistent(&book);

            let mut replacement = ReplaceOrder {
                order_id: OrderId(1),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(4),
                price: PriceTicks(100),
                quantity: Quantity(5),
            };
            book.replace(replacement).expect("reduce");
            assert_eq!(
                book.top_level(side).expect("level").aggregate_quantity,
                u128::from(u64::MAX) + 5
            );
            assert_index_consistent(&book);
            replacement.sequence = SequenceNumber(5);
            replacement.quantity = Quantity(u64::MAX);
            book.replace(replacement).expect("increase");
            assert_index_consistent(&book);
            replacement.sequence = SequenceNumber(6);
            replacement.price = PriceTicks(if side == Side::Buy { 99 } else { 101 });
            book.replace(replacement).expect("reprice");
            assert_index_consistent(&book);

            let restored = OrderBook::from_state(InstrumentId(1), &book.export_state())
                .expect("restore totals");
            assert_index_consistent(&restored);
            assert_eq!(restored.top_level(side), book.top_level(side));
            for id in [2, 1] {
                book.cancel(CancelOrder {
                    order_id: OrderId(id),
                    account_id: AccountId(1),
                    instrument_id: InstrumentId(1),
                    sequence: SequenceNumber(7),
                })
                .expect("cancel");
                assert_index_consistent(&book);
            }
            assert_eq!(book.top_level(side), None);
            book.submit(order(8, 100, 3, side), &mut reports)
                .expect("reuse level");
            assert_eq!(book.top_level(side).expect("level").aggregate_quantity, 3);
            assert_index_consistent(&book);
        }
    }

    #[test]
    fn report_capacity_preflight_preserves_book() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("first ask");
        reports.clear();
        book.submit(order(2, 100, 1, Side::Sell), &mut reports)
            .expect("second ask");
        reports.clear();
        let digest = book.stable_digest();
        assert_eq!(
            book.submit(order(3, 100, 2, Side::Buy), &mut reports),
            Err(RejectReason::ReportCapacity)
        );
        assert_eq!(book.stable_digest(), digest);
    }

    #[test]
    fn deterministic_runs_match_digest() {
        let mut first = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut second = OrderBook::<4, 4>::new(InstrumentId(1));
        for book in [&mut first, &mut second] {
            let mut reports = ReportBuffer::<4>::new();
            for input in [
                order(1, 99, 2, Side::Buy),
                order(2, 101, 3, Side::Sell),
                order(3, 101, 1, Side::Buy),
            ] {
                reports.clear();
                book.submit(input, &mut reports).expect("valid order");
            }
        }
        assert_eq!(first.stable_digest(), second.stable_digest());
    }

    #[test]
    fn cancel_requires_owner_and_preserves_fifo() {
        let mut book = OrderBook::<2, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        for resting in [
            order(1, 100, 1, Side::Sell),
            order(2, 100, 1, Side::Sell),
            order(3, 100, 1, Side::Sell),
        ] {
            reports.clear();
            book.submit(resting, &mut reports).expect("rest order");
        }
        assert_eq!(
            book.cancel(CancelOrder {
                order_id: OrderId(2),
                account_id: AccountId(9),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(4),
            }),
            Err(RejectReason::NotOrderOwner)
        );
        book.cancel(CancelOrder {
            order_id: OrderId(2),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(4),
        })
        .expect("owner cancel");
        reports.clear();
        book.submit(order(4, 100, 2, Side::Buy), &mut reports)
            .expect("cross remaining FIFO");
        let makers: std::vec::Vec<_> = reports.iter().map(|report| report.maker_order_id).collect();
        assert_eq!(makers, [OrderId(1), OrderId(3)]);
    }

    #[test]
    fn index_handles_collisions_shifts_and_slot_reuse() {
        let mut book = OrderBook::<2, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        for resting in [
            order(1, 100, 1, Side::Sell),
            order(33, 100, 1, Side::Sell),
            order(65, 100, 1, Side::Sell),
        ] {
            book.submit(resting, &mut reports).expect("rest order");
            reports.clear();
        }
        assert_index_consistent(&book);

        book.cancel(CancelOrder {
            order_id: OrderId(33),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(66),
        })
        .expect("cancel colliding middle order");
        assert_index_consistent(&book);

        book.submit(order(97, 100, 1, Side::Buy), &mut reports)
            .expect("fill front order");
        assert_eq!(
            reports.iter().next().map(|report| report.maker_order_id),
            Some(OrderId(1))
        );
        assert_index_consistent(&book);

        book.cancel(CancelOrder {
            order_id: OrderId(65),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(98),
        })
        .expect("cancel shifted order");
        reports.clear();
        book.submit(order(129, 100, 1, Side::Sell), &mut reports)
            .expect("reuse index slot");
        assert_index_consistent(&book);
    }

    #[test]
    fn stable_handles_survive_unrelated_mutation() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        for resting in [
            order(1, 100, 1, Side::Sell),
            order(2, 100, 1, Side::Sell),
            order(10, 101, 1, Side::Sell),
            order(11, 101, 1, Side::Sell),
            order(20, 99, 1, Side::Buy),
        ] {
            book.submit(resting, &mut reports).expect("rest order");
            reports.clear();
        }
        let before = book.index.location(OrderId(11));
        assert!(before.is_some());

        book.cancel(CancelOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(30),
        })
        .expect("cancel level A head");
        assert_index_consistent(&book);
        let fill = book
            .submit(order(21, 100, 1, Side::Buy), &mut reports)
            .expect("cross level A tail");
        assert_eq!(fill.state, OrderState::Filled);
        reports.clear();
        assert_index_consistent(&book);
        book.submit(order(12, 102, 1, Side::Sell), &mut reports)
            .expect("open level C");
        reports.clear();
        book.cancel(CancelOrder {
            order_id: OrderId(20),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(31),
        })
        .expect("cancel bid level");
        assert_index_consistent(&book);

        assert_eq!(
            book.index.location(OrderId(11)),
            before,
            "unrelated mutations preserve the stable handle"
        );
        book.cancel(CancelOrder {
            order_id: OrderId(11),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(32),
        })
        .expect("cancel through preserved handle");
        assert_index_consistent(&book);
    }

    #[test]
    fn stale_handles_fail_closed() {
        let mut book = OrderBook::<2, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("rest order");
        reports.clear();
        let cancel = |sequence: u64| CancelOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(sequence),
        };

        // Corrupt the recorded location so it points at a free slot.
        let location = book.index.location(OrderId(1)).expect("indexed order");
        let flat = book.index.find_slot(OrderId(1)).expect("index slot");
        let stale = OrderLocation {
            slot: location.slot + 1,
            ..location
        };
        *book.index.slot_mut(flat) = IndexSlot {
            order_id: OrderId(1),
            location: OrderIndex::<2, 4>::encode_location(stale),
        };
        assert_eq!(book.locate(OrderId(1)), None);
        assert_eq!(book.cancel(cancel(2)), Err(RejectReason::UnknownOrder));
        let level = book.asks[location.level_index].as_mut().expect("level");
        assert_eq!(level.unlink(stale.slot), None);
        assert_eq!(level.len, 1, "failed unlink left the level untouched");

        // A legitimate removal invalidates the handle: a repeat cancel fails.
        *book.index.slot_mut(flat) = IndexSlot {
            order_id: OrderId(1),
            location: OrderIndex::<2, 4>::encode_location(location),
        };
        book.cancel(cancel(3)).expect("first cancel succeeds");
        assert_eq!(book.cancel(cancel(4)), Err(RejectReason::UnknownOrder));
        assert_index_consistent(&book);
    }

    #[test]
    fn full_level_rejects_atomically() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("first order");
        reports.clear();
        book.submit(order(2, 100, 1, Side::Sell), &mut reports)
            .expect("second order");
        reports.clear();
        let digest = book.stable_digest();
        assert_eq!(
            book.submit(order(3, 100, 1, Side::Sell), &mut reports),
            Err(RejectReason::PriceLevelOrderCapacity)
        );
        assert_eq!(book.stable_digest(), digest);
        assert_index_consistent(&book);
    }

    #[test]
    fn back_shift_updates_reverse_slots_across_sides() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        book.submit(order(1, 101, 1, Side::Sell), &mut reports)
            .expect("rest sell");
        book.submit(order(17, 100, 1, Side::Buy), &mut reports)
            .expect("rest colliding buy");
        assert_index_consistent(&book);

        book.cancel(CancelOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(18),
        })
        .expect("cancel sell before colliding buy");
        assert_index_consistent(&book);

        book.cancel(CancelOrder {
            order_id: OrderId(17),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(19),
        })
        .expect("cancel relocated buy");
        assert_index_consistent(&book);
    }

    fn dump_book<const LEVELS: usize, const ORDERS: usize>(
        book: &OrderBook<LEVELS, ORDERS>,
    ) -> hft_model::ModelDump {
        let mut dump = std::vec::Vec::new();
        for (side, levels) in [(Side::Buy, &book.bids), (Side::Sell, &book.asks)] {
            let mut prices: std::vec::Vec<i64> =
                levels.iter().flatten().map(|level| level.price.0).collect();
            prices.sort_unstable();
            for price in prices {
                let level = levels
                    .iter()
                    .flatten()
                    .find(|level| level.price.0 == price)
                    .expect("dump level");
                level.for_each_live(|order| {
                    dump.push((side, price, order.id.0, order.quantity.0, order.sequence.0));
                });
            }
        }
        dump
    }

    fn assert_side_index_consistent<const LEVELS: usize, const ORDERS: usize>(
        levels: &[Option<PriceLevel<ORDERS>>; LEVELS],
        index: &LevelIndex<LEVELS>,
        descending: bool,
    ) {
        // Sorted in side order with unique prices.
        for window in index.entries[..index.len].windows(2) {
            let (best, worse) = (window[0].0.0, window[1].0.0);
            if descending {
                assert!(best > worse, "bid index not strictly descending");
            } else {
                assert!(best < worse, "ask index not strictly ascending");
            }
        }
        // Occupied and free slots partition the level array exactly once.
        assert_eq!(index.len + index.free_len, LEVELS, "slot sets cover levels");
        let mut indexed = vec![false; LEVELS];
        for &(price, slot) in index.iter() {
            assert!(slot < LEVELS, "indexed slot in range");
            assert!(!indexed[slot], "slot {slot} indexed twice");
            indexed[slot] = true;
            let level = levels[slot].as_ref().expect("indexed level occupied");
            assert_eq!(level.price, price, "indexed price matches the level");
        }
        let mut freed = vec![false; LEVELS];
        for &slot in &index.free[..index.free_len] {
            assert!(slot < LEVELS, "free slot in range");
            assert!(!freed[slot], "slot {slot} freed twice");
            assert!(!indexed[slot], "slot {slot} both free and indexed");
            freed[slot] = true;
        }
        for (slot, level) in levels.iter().enumerate() {
            assert_eq!(
                level.is_some(),
                indexed[slot],
                "slot {slot} occupancy matches index"
            );
        }
    }

    fn assert_level_index_consistent<const LEVELS: usize, const ORDERS: usize>(
        book: &OrderBook<LEVELS, ORDERS>,
    ) {
        assert_side_index_consistent(&book.bids, &book.bid_levels, true);
        assert_side_index_consistent(&book.asks, &book.ask_levels, false);
    }

    #[test]
    fn best_bid_ask_match_sorted_index() {
        let mut book = OrderBook::<8, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        for (id, price, side) in [
            (1, 100, Side::Sell),
            (2, 105, Side::Sell),
            (3, 99, Side::Buy),
            (4, 95, Side::Buy),
            (5, 110, Side::Sell),
            (6, 90, Side::Buy),
        ] {
            book.submit(order(id, price, 1, side), &mut reports)
                .expect("rest order");
            reports.clear();
        }
        assert_level_index_consistent(&book);
        // Best bid should be 99 (highest buy).
        assert_eq!(
            book.best_crossing_level(Side::Buy, PriceTicks(200)),
            Some(0),
            "best bid"
        );
        // Best ask should be 100 (lowest sell).
        assert_eq!(
            book.best_crossing_level(Side::Sell, PriceTicks(50)),
            Some(0),
            "best ask"
        );
    }

    #[test]
    fn empty_levels_produce_no_crossing() {
        let book = OrderBook::<4, 2>::new(InstrumentId(1));
        assert_eq!(book.best_crossing_level(Side::Buy, PriceTicks(100)), None);
        assert_eq!(book.best_crossing_level(Side::Sell, PriceTicks(1)), None);
    }

    #[test]
    fn boundary_price_only_crosses_correct_side() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        // Create separate levels so both sides persist.
        book.submit(order(1, 100, 2, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        book.submit(order(2, 99, 2, Side::Buy), &mut reports)
            .expect("rest bid");
        reports.clear();
        assert_level_index_consistent(&book);
        // Buy at exactly 100 crosses the ask at 100.
        assert_eq!(
            book.best_crossing_level(Side::Buy, PriceTicks(100)),
            Some(0)
        );
        // Sell at exactly 99 crosses the bid at 99.
        assert_eq!(
            book.best_crossing_level(Side::Sell, PriceTicks(99)),
            Some(0)
        );
        // Buy at 99 does not cross ask at 100.
        assert_eq!(book.best_crossing_level(Side::Buy, PriceTicks(99)), None);
        // Sell at 100 does not cross bid at 99.
        assert_eq!(book.best_crossing_level(Side::Sell, PriceTicks(100)), None);
    }

    #[test]
    fn level_index_survives_churn() {
        let mut book = OrderBook::<8, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<16>::new();
        // Create levels at prices 98, 99, 100, 101, 102.
        for (id, price) in [(1, 98), (2, 99), (3, 100), (4, 101), (5, 102)] {
            book.submit(order(id, price, 2, Side::Sell), &mut reports)
                .expect("rest");
            reports.clear();
        }
        book.submit(order(6, 97, 2, Side::Buy), &mut reports)
            .expect("rest bid");
        reports.clear();
        assert_level_index_consistent(&book);
        // Cancel the middle ask level (100).
        book.cancel(CancelOrder {
            order_id: OrderId(3),
            account_id: AccountId(1),
            instrument_id: InstrumentId(1),
            sequence: SequenceNumber(10),
        })
        .expect("cancel middle");
        assert_level_index_consistent(&book);
        // Fill the 98 ask level completely.
        book.submit(order(7, 100, 2, Side::Buy), &mut reports)
            .expect("cross 98");
        reports.clear();
        assert_level_index_consistent(&book);
        // The best ask is now 99 (98 was fully filled, 100 was cancelled).
        let best = book.best_crossing_level(Side::Buy, PriceTicks(200));
        assert!(best.is_some(), "best ask after churn");
        let level = book.asks[best.unwrap()].as_ref().unwrap();
        assert_eq!(level.price.0, 99, "best ask price after churn");
    }

    #[test]
    fn no_skipped_liquidity() {
        let mut book = OrderBook::<8, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<16>::new();
        for (id, price, qty) in [(1, 100, 5), (2, 101, 5), (3, 102, 5)] {
            book.submit(order(id, price, qty, Side::Sell), &mut reports)
                .expect("rest");
            reports.clear();
        }
        // Submit a buy that should consume all three levels in price order.
        let summary = book
            .submit(order(4, 200, 15, Side::Buy), &mut reports)
            .expect("cross all");
        assert_eq!(summary.filled_quantity, Quantity(15));
        let makers: std::vec::Vec<_> = reports.iter().map(|r| r.maker_order_id.0).collect();
        assert_eq!(makers, [1, 2, 3], "must consume levels in price order");
    }

    #[test]
    fn model_equivalent_digest_after_indexed_discovery() {
        // Build two books with the same orders; digest must match.
        let mut first = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut second = OrderBook::<4, 4>::new(InstrumentId(1));
        for book in [&mut first, &mut second] {
            let mut reports = ReportBuffer::<4>::new();
            for input in [
                order(1, 99, 2, Side::Buy),
                order(2, 101, 3, Side::Sell),
                order(3, 101, 1, Side::Buy),
            ] {
                reports.clear();
                book.submit(input, &mut reports).expect("valid order");
            }
            assert_level_index_consistent(book);
        }
        assert_eq!(first.stable_digest(), second.stable_digest());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn generated_commands_match_array_model() {
        const LEVELS: usize = 4;
        const ORDERS: usize = 4;
        const REPORTS: usize = 8;
        let mut book = OrderBook::<LEVELS, ORDERS>::new(InstrumentId(1));
        let mut model = hft_model::ModelBook::default();
        let mut reports = ReportBuffer::<REPORTS>::new();
        let mut generator = hft_model::CommandGen::new(
            hft_model::GenConfig {
                accounts: 2,
                minimum_price: 99,
                maximum_price: 101,
                max_quantity: 3,
                cancel_probability_pct: 60,
                duplicate_id_probability_pct: 10,
                ioc_probability_pct: 15,
                fok_probability_pct: 10,
                post_only_probability_pct: 10,
                replace_probability_pct: 25,
            },
            InstrumentId(1),
            0x5eed,
        );
        for step in 0..600_u64 {
            if step >= 9 {
                eprintln!("s{step} book={:?}", dump_book(&book));
                eprintln!("s{step} model={:?}", hft_model::dump_model(&model));
            }
            let command = generator.next_command();
            match command {
                hft_model::Command::New(command) => {
                    reports.clear();
                    let actual = book.submit(command, &mut reports);
                    let expected = model.submit(&command, REPORTS, LEVELS, ORDERS);
                    match (actual, expected) {
                        (Ok(summary), Ok((state, filled, resting, _discarded, fills))) => {
                            assert_eq!(summary.state, state, "state at step {step}");
                            assert_eq!(summary.filled_quantity, filled, "filled at step {step}");
                            assert_eq!(summary.resting_quantity, resting, "resting at step {step}");
                            let actual_fills: std::vec::Vec<_> = reports
                                .iter()
                                .map(|report| {
                                    (report.maker_order_id, report.price, report.quantity)
                                })
                                .collect();
                            let expected_fills: std::vec::Vec<_> = fills
                                .iter()
                                .map(|fill| (fill.maker_order_id, fill.price, fill.quantity))
                                .collect();
                            assert_eq!(actual_fills, expected_fills, "fills at step {step}");
                        }
                        (Err(actual_error), Err(expected_error)) => {
                            assert_eq!(
                                actual_error, expected_error,
                                "submit rejection at step {step}"
                            );
                        }
                        (actual, expected) => {
                            panic!("submit divergence at step {step}: {actual:?} vs {expected:?}");
                        }
                    }
                }
                hft_model::Command::Cancel(command) => {
                    let actual = book.cancel(command);
                    let expected = model.cancel(&command);
                    match (actual, expected) {
                        (Ok(cancelled), Ok((id, account, quantity))) => {
                            assert_eq!(cancelled.order_id.0, id, "cancel id at step {step}");
                            assert_eq!(
                                cancelled.account_id.0, account,
                                "cancel account at step {step}"
                            );
                            assert_eq!(
                                cancelled.quantity.0, quantity,
                                "cancel quantity at step {step}"
                            );
                        }
                        (Err(actual_error), Err(expected_error)) => {
                            assert_eq!(
                                actual_error, expected_error,
                                "cancel rejection at step {step}"
                            );
                        }
                        (actual, expected) => {
                            panic!("cancel divergence at step {step}: {actual:?} vs {expected:?}");
                        }
                    }
                }
                hft_model::Command::Replace(command) => {
                    let actual = book.replace(command);
                    let expected = model.replace(&command, LEVELS, ORDERS);
                    match (actual, expected) {
                        (Ok(replaced), Ok((old_quantity, priority_lost))) => {
                            assert_eq!(
                                replaced.old_quantity.0, old_quantity,
                                "replace old quantity at step {step}"
                            );
                            assert_eq!(
                                replaced.priority_lost, priority_lost,
                                "priority retention at step {step}"
                            );
                            assert_eq!(replaced.new_quantity.0, command.quantity.0);
                        }
                        (Err(actual_error), Err(expected_error)) => {
                            assert_eq!(
                                actual_error, expected_error,
                                "replace rejection at step {step}"
                            );
                        }
                        (actual, expected) => {
                            panic!("replace divergence at step {step}: {actual:?} vs {expected:?}");
                        }
                    }
                }
            }
            assert_eq!(
                dump_book(&book),
                hft_model::dump_model(&model),
                "book state at step {step}"
            );
            assert_index_consistent(&book);
        }
    }

    #[test]
    fn duplicate_resting_id_is_rejected_without_mutation() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<2>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("rest ask");
        let digest = book.stable_digest();
        assert_eq!(
            book.submit(order(1, 101, 1, Side::Sell), &mut reports),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(book.stable_digest(), digest);
        assert_index_consistent(&book);
    }

    #[test]
    fn crossing_stops_at_the_taker_price_limit() {
        let mut book = OrderBook::<8, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        for (id, price) in [(1, 100), (2, 105), (3, 110)] {
            book.submit(order(id, price, 2, Side::Sell), &mut reports)
                .expect("rest ask");
            reports.clear();
        }
        // A buy at 105 must not touch the 110 ask.
        let summary = book
            .submit(order(4, 105, 4, Side::Buy), &mut reports)
            .expect("cross two levels");
        assert_eq!(summary.state, OrderState::Filled);
        let makers: std::vec::Vec<_> = reports.iter().map(|report| report.maker_order_id).collect();
        assert_eq!(makers, [OrderId(1), OrderId(2)]);
        assert_eq!(book.order_count(), 1);
        assert_index_consistent(&book);
        assert_level_index_consistent(&book);
    }

    #[test]
    fn partial_cross_then_rest_keeps_price_time_position() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 5, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        // Taker fills the 5 and rests the remaining 2 as a bid at 100.
        let summary = book
            .submit(order(2, 100, 7, Side::Buy), &mut reports)
            .expect("partial cross then rest");
        assert_eq!(summary.state, OrderState::PartiallyFilled);
        assert_eq!(summary.resting_quantity, Quantity(2));
        reports.clear();
        // A second bid at the same price queues behind the rested taker.
        book.submit(order(3, 100, 1, Side::Buy), &mut reports)
            .expect("join bid level");
        reports.clear();
        // A sell at 100 fills the rested taker (2), then the queued bid (1 of
        // 1), and rests its own remainder: price-time order within the level.
        book.submit(order(4, 100, 4, Side::Sell), &mut reports)
            .expect("cross bid level");
        let makers: std::vec::Vec<_> = reports.iter().map(|report| report.maker_order_id).collect();
        assert_eq!(makers, [OrderId(2), OrderId(3)]);
        assert_index_consistent(&book);
    }

    #[test]
    fn exact_report_capacity_fit_succeeds() {
        let mut book = OrderBook::<4, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<2>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("first ask");
        reports.clear();
        book.submit(order(2, 101, 1, Side::Sell), &mut reports)
            .expect("second ask");
        reports.clear();
        let summary = book
            .submit(order(3, 101, 2, Side::Buy), &mut reports)
            .expect("both fills exactly fit the report buffer");
        assert_eq!(summary.state, OrderState::Filled);
        assert_eq!(summary.report_count, 2);
        assert_index_consistent(&book);
    }

    #[test]
    fn emptied_level_slot_is_reused_by_later_rest() {
        let mut book = OrderBook::<4, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 2, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        // Fully fill the only ask level: it is removed and its slot freed.
        book.submit(order(2, 100, 2, Side::Buy), &mut reports)
            .expect("fill the level");
        reports.clear();
        assert_level_index_consistent(&book);
        // Rest at the same price again: the freed slot is reused.
        book.submit(order(3, 100, 1, Side::Sell), &mut reports)
            .expect("re-rest at the same price");
        assert_eq!(book.order_count(), 1);
        assert_index_consistent(&book);
        assert_level_index_consistent(&book);
    }

    #[test]
    fn rejected_rest_levels_and_reports_stay_untouched() {
        let mut book = OrderBook::<2, 2>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        for (id, price) in [(1, 101), (2, 102)] {
            book.submit(order(id, price, 1, Side::Sell), &mut reports)
                .expect("rest ask");
            reports.clear();
        }
        // Both level slots are occupied: a new resting price must reject.
        let digest = book.stable_digest();
        assert_eq!(
            book.submit(order(3, 99, 1, Side::Sell), &mut reports),
            Err(RejectReason::PriceLevelCapacity)
        );
        assert_eq!(reports.len(), 0);
        assert_eq!(book.stable_digest(), digest);
        assert_index_consistent(&book);
        assert_level_index_consistent(&book);
    }

    #[test]
    fn shifted_orders_stay_locatable_and_cancellable() {
        let mut book = OrderBook::<8, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        for id in 1..=16_u64 {
            let side = if id % 2 == 0 { Side::Sell } else { Side::Buy };
            let price = if side == Side::Sell { 101 } else { 99 };
            book.submit(order(id, price, 1, side), &mut reports)
                .expect("rest");
            reports.clear();
        }
        // Cancels in id order force index back-shifts; later cancels of the
        // shifted entries must still locate them.
        for id in 1..=8 {
            book.cancel(CancelOrder {
                order_id: OrderId(id),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(id + 100),
            })
            .unwrap_or_else(|error| panic!("cancel {id}: {error:?}"));
        }
        for id in 9..=16 {
            book.cancel(CancelOrder {
                order_id: OrderId(id),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(id + 100),
            })
            .unwrap_or_else(|error| panic!("cancel shifted {id}: {error:?}"));
        }
        assert_eq!(book.order_count(), 0);
        assert_index_consistent(&book);
    }

    #[test]
    fn ioc_never_rests_and_conserves_quantity() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 3, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        // Partial: fills 3 of 5, discards 2.
        let mut ioc = order(2, 100, 5, Side::Buy);
        ioc.time_in_force = TimeInForce::Ioc;
        let summary = book.submit(ioc, &mut reports).expect("ioc partial");
        assert_eq!(summary.filled_quantity, Quantity(3));
        assert_eq!(summary.resting_quantity, Quantity(0));
        assert_eq!(summary.state, OrderState::PartiallyFilled);
        assert_eq!(book.order_count(), 0, "nothing rests behind an IOC");
        // Zero fill against a non-crossing market still consumes nothing.
        book.submit(order(3, 100, 4, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        let mut empty = order(4, 90, 2, Side::Buy);
        empty.time_in_force = TimeInForce::Ioc;
        let summary = book.submit(empty, &mut reports).expect("ioc empty");
        assert_eq!(summary.filled_quantity, Quantity(0));
        assert_eq!(summary.resting_quantity, Quantity(0));
        assert_eq!(summary.state, OrderState::Accepted);
        assert_eq!(book.order_count(), 1, "only the resting ask remains");
    }

    #[test]
    fn fok_is_atomic_across_liquidity_and_capacity() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 3, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        let digest = book.stable_digest();
        // Insufficient liquidity rejects without mutation.
        let mut short = order(2, 100, 5, Side::Buy);
        short.time_in_force = TimeInForce::Fok;
        assert_eq!(
            book.submit(short, &mut reports),
            Err(RejectReason::InsufficientLiquidity)
        );
        assert_eq!(book.stable_digest(), digest, "FOK reject mutates nothing");
        assert!(reports.is_empty());
        // Report capacity below the required fill count also rejects.
        let mut wide = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut small_reports = ReportBuffer::<1>::new();
        for id in 1..=2_u64 {
            wide.submit(
                order(
                    id,
                    100 + i64::try_from(id).expect("small id"),
                    1,
                    Side::Sell,
                ),
                &mut small_reports,
            )
            .expect("rest level");
            small_reports.clear();
        }
        let mut two_level = order(9, 200, 2, Side::Buy);
        two_level.time_in_force = TimeInForce::Fok;
        assert_eq!(
            wide.submit(two_level, &mut small_reports),
            Err(RejectReason::ReportCapacity),
            "FOK needs capacity for every fill"
        );
        assert_eq!(wide.order_count(), 2, "both makers untouched");
        // Full liquidity executes completely.
        let mut ok = order(3, 100, 3, Side::Buy);
        ok.time_in_force = TimeInForce::Fok;
        let summary = book.submit(ok, &mut reports).expect("fok full");
        assert_eq!(summary.filled_quantity, Quantity(3));
        assert_eq!(summary.resting_quantity, Quantity(0));
        assert_eq!(summary.state, OrderState::Filled);
        assert_eq!(reports.len(), 1);
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn post_only_crossing_rejects_without_mutation() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 100, 3, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        let digest = book.stable_digest();
        // Buy at the ask would trade: reject before any mutation.
        let mut po = order(2, 100, 5, Side::Buy);
        po.time_in_force = TimeInForce::PostOnly;
        assert_eq!(
            book.submit(po, &mut reports),
            Err(RejectReason::PostOnlyWouldTrade)
        );
        assert_eq!(book.stable_digest(), digest);
        assert!(reports.is_empty());
        // Below the market it cannot trade and rests normally.
        let mut quiet = order(3, 99, 2, Side::Buy);
        quiet.time_in_force = TimeInForce::PostOnly;
        let summary = book.submit(quiet, &mut reports).expect("post only rests");
        assert_eq!(summary.filled_quantity, Quantity(0));
        assert_eq!(summary.resting_quantity, Quantity(2));
        assert_eq!(summary.state, OrderState::Accepted);
    }

    #[test]
    fn post_only_accepted_order_joins_the_fifo_tail() {
        let mut book = OrderBook::<4, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        book.submit(order(1, 100, 1, Side::Sell), &mut reports)
            .expect("first ask");
        reports.clear();
        let mut po = order(2, 100, 1, Side::Sell);
        po.time_in_force = TimeInForce::PostOnly;
        book.submit(po, &mut reports)
            .expect("post only joins level");
        reports.clear();
        book.submit(order(3, 100, 2, Side::Buy), &mut reports)
            .expect("sweep level");
        let makers: std::vec::Vec<_> = reports
            .iter()
            .map(|report| report.maker_order_id.0)
            .collect();
        assert_eq!(makers, [1, 2], "post-only order rests behind the elder");
    }

    #[test]
    fn post_only_rests_into_an_empty_book() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        let mut po = order(1, 100, 3, Side::Sell);
        po.time_in_force = TimeInForce::PostOnly;
        let summary = book.submit(po, &mut reports).expect("rests on empty side");
        assert_eq!(summary.state, OrderState::Accepted);
        assert_eq!(book.order_count(), 1);
    }

    #[test]
    fn logical_state_round_trip_preserves_fifo_and_digest() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        for input in [
            order(1, 99, 2, Side::Buy),
            order(2, 99, 3, Side::Buy),
            order(3, 101, 4, Side::Sell),
        ] {
            book.submit(input, &mut reports).expect("rest order");
            reports.clear();
        }

        let state = book.export_state();
        let restored = OrderBook::from_state(InstrumentId(1), &state).expect("restore state");

        assert_eq!(restored.export_state(), state);
        assert_eq!(restored.stable_digest(), book.stable_digest());
    }

    #[test]
    fn logical_state_rejects_duplicate_and_crossed_orders() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 99, 2, Side::Buy), &mut reports)
            .expect("bid");
        reports.clear();
        book.submit(order(2, 101, 2, Side::Sell), &mut reports)
            .expect("ask");
        let mut duplicate = book.export_state();
        duplicate.asks[0].orders[0].order_id = OrderId(1);
        assert!(matches!(
            OrderBook::from_state(InstrumentId(1), &duplicate),
            Err(BookStateError::DuplicateOrderId)
        ));

        let mut crossed = book.export_state();
        crossed.bids[0].price = PriceTicks(101);
        crossed.bids[0].orders[0].price = PriceTicks(101);
        assert!(matches!(
            OrderBook::from_state(InstrumentId(1), &crossed),
            Err(BookStateError::Crossed)
        ));
    }

    #[test]
    fn logical_state_rejects_fifo_and_global_sequence_reuse() {
        let mut book = OrderBook::<4, 4>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<4>::new();
        book.submit(order(1, 99, 2, Side::Buy), &mut reports)
            .expect("first bid");
        book.submit(order(2, 99, 2, Side::Buy), &mut reports)
            .expect("second bid");
        book.submit(order(3, 98, 2, Side::Buy), &mut reports)
            .expect("third bid");

        let mut reversed = book.export_state();
        reversed.bids[0].orders.swap(0, 1);
        assert!(matches!(
            OrderBook::from_state(InstrumentId(1), &reversed),
            Err(BookStateError::Sequence)
        ));

        let mut duplicate = book.export_state();
        duplicate.bids[1].orders[0].sequence = duplicate.bids[0].orders[0].sequence;
        assert!(matches!(
            OrderBook::from_state(InstrumentId(1), &duplicate),
            Err(BookStateError::Sequence)
        ));
    }
}
