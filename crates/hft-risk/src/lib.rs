#![forbid(unsafe_code)]

use core::num::NonZeroUsize;
use hft_types::{AccountId, NewOrder, OrderId, PriceTicks, Quantity, RejectReason, Side};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskLimits {
    pub max_quantity: Quantity,
    pub max_notional: u128,
    pub max_abs_position: Quantity,
    pub max_open_orders: u32,
    pub minimum_price: PriceTicks,
    pub maximum_price: PriceTicks,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationError {
    DuplicateAccount,
    AccountCapacity,
    InvalidLimits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountRiskState {
    pub id: AccountId,
    pub limits: RiskLimits,
    pub settled_position: i128,
    pub reserved_buys: u128,
    pub reserved_sells: u128,
    pub open_orders: u32,
    pub killed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationRiskState {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub side: Side,
    pub quantity: Quantity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskEngineState {
    pub accounts: Vec<AccountRiskState>,
    pub reservations: Vec<ReservationRiskState>,
    pub maximum_order_id: Option<OrderId>,
    pub killed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskStateError {
    AccountCapacity,
    ReservationCapacity,
    DuplicateAccount,
    DuplicateOrder,
    NonCanonical,
    InvalidLimits,
    UnknownAccount,
    InvalidWatermark,
    ExposureMismatch,
    OpenOrderMismatch,
    PositionLimit,
    ArithmeticOverflow,
    InvalidQuantity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AccountState {
    id: AccountId,
    limits: RiskLimits,
    settled_position: i128,
    reserved_buys: u128,
    reserved_sells: u128,
    open_orders: u32,
    killed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Reservation {
    order_id: OrderId,
    account_id: AccountId,
    index_slot: u32,
    side: Side,
    quantity: Quantity,
}

const NIL: usize = usize::MAX;

/// Two planes per index keep the load factor at or below 1/2.
const INDEX_PLANES: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndexSlot {
    Empty,
    Occupied { key: u64, value: NonZeroUsize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeError {
    Full,
    Duplicate,
}

/// Stored handles use `slot + 1` so zero encodes an empty entry. Keys keep
/// their full range. `usize::MAX` remains the free-list terminator.
/// Occupancy stays at or below half capacity. Nested planes avoid a const
/// generic product in the array length.
#[derive(Debug)]
struct ProbeIndex<const PLANE: usize, const PLANES: usize> {
    slots: [[IndexSlot; PLANE]; PLANES],
}

impl<const PLANE: usize, const PLANES: usize> ProbeIndex<PLANE, PLANES> {
    const CAPACITY: usize = PLANE * PLANES;

    const fn new() -> Self {
        Self {
            slots: [[IndexSlot::Empty; PLANE]; PLANES],
        }
    }

    /// Flat-index coordinates. Callers only pass indices below `CAPACITY`.
    fn coordinates(flat_index: usize) -> (usize, usize) {
        debug_assert!(flat_index < Self::CAPACITY);
        (flat_index / PLANE, flat_index % PLANE)
    }

    fn slot(&self, flat_index: usize) -> &IndexSlot {
        let (plane, within) = Self::coordinates(flat_index);
        &self.slots[plane][within]
    }

    fn slot_mut(&mut self, flat_index: usize) -> &mut IndexSlot {
        let (plane, within) = Self::coordinates(flat_index);
        &mut self.slots[plane][within]
    }

    /// Probing requires a non-zero capacity; callers guard `CAPACITY == 0`.
    fn probe_start(key: u64) -> usize {
        let capacity = u64::try_from(Self::CAPACITY).unwrap_or(u64::MAX);
        let mixed = key.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        usize::try_from(mixed % capacity).unwrap_or(0)
    }

    fn probe_distance(home: usize, current: usize) -> usize {
        if current >= home {
            current - home
        } else {
            Self::CAPACITY - (home - current)
        }
    }

    fn find_slot(&self, key: u64) -> Option<usize> {
        if Self::CAPACITY == 0 {
            return None;
        }
        let start = Self::probe_start(key);
        for offset in 0..Self::CAPACITY {
            let flat_index = start.wrapping_add(offset) % Self::CAPACITY;
            match self.slot(flat_index) {
                IndexSlot::Empty => return None,
                IndexSlot::Occupied { key: indexed, .. } if *indexed == key => {
                    return Some(flat_index);
                }
                IndexSlot::Occupied { .. } => {}
            }
        }
        None
    }

    fn lookup(&self, key: u64) -> Option<usize> {
        match self.slot(self.find_slot(key)?) {
            IndexSlot::Occupied { value, .. } => Some(value.get() - 1),
            IndexSlot::Empty => None,
        }
    }

    fn insert(&mut self, key: u64, value: usize) -> Result<u32, ProbeError> {
        if Self::CAPACITY == 0 {
            return Err(ProbeError::Full);
        }
        let value = value
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(ProbeError::Full)?;
        let start = Self::probe_start(key);
        for offset in 0..Self::CAPACITY {
            let flat_index = start.wrapping_add(offset) % Self::CAPACITY;
            match self.slot(flat_index) {
                IndexSlot::Empty => {
                    let flat_u32 = u32::try_from(flat_index).map_err(|_| ProbeError::Full)?;
                    *self.slot_mut(flat_index) = IndexSlot::Occupied { key, value };
                    return Ok(flat_u32);
                }
                IndexSlot::Occupied { key: indexed, .. } if *indexed == key => {
                    return Err(ProbeError::Duplicate);
                }
                IndexSlot::Occupied { .. } => {}
            }
        }
        Err(ProbeError::Full)
    }

    /// Removes `key` at `flat_index` and closes the probe hole by shifting
    /// displaced entries back, reporting each move through `update_moved`.
    /// Fails closed on a stale handle.
    fn remove_at(
        &mut self,
        flat_index: u32,
        key: u64,
        mut update_moved: impl FnMut(u64, usize, u32) -> bool,
    ) -> Option<usize> {
        let flat_index = usize::try_from(flat_index).ok()?;
        if flat_index >= Self::CAPACITY {
            return None;
        }
        let IndexSlot::Occupied {
            key: indexed,
            value,
        } = *self.slot(flat_index)
        else {
            return None;
        };
        if indexed != key {
            return None;
        }
        let mut hole = flat_index;
        let mut candidate = hole.wrapping_add(1) % Self::CAPACITY;
        loop {
            let entry = *self.slot(candidate);
            let IndexSlot::Occupied {
                key: candidate_key,
                value: candidate_value,
            } = entry
            else {
                *self.slot_mut(hole) = IndexSlot::Empty;
                break;
            };
            let home_bucket = Self::probe_start(candidate_key);
            if Self::probe_distance(home_bucket, hole)
                < Self::probe_distance(home_bucket, candidate)
            {
                *self.slot_mut(hole) = entry;
                let new_flat = u32::try_from(hole).expect("occupied slots fit u32");
                // The closure must run in every profile; skipping it in
                // release strands the moved value's stored handle.
                let updated = update_moved(candidate_key, candidate_value.get() - 1, new_flat);
                debug_assert!(updated);
                hole = candidate;
            }
            candidate = candidate.wrapping_add(1) % Self::CAPACITY;
        }
        Some(value.get() - 1)
    }
}

/// Reservation storage slot: a live reservation or a free-list link. Live
/// and free sets are disjoint and slot handles stay stable under churn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationSlot {
    Free { next_free: usize },
    Live(Reservation),
}

/// Deterministic, fixed-capacity pre-trade risk state.
///
/// Position is conservative: buy and sell reservations are tracked separately
/// so opposing open orders cannot cancel worst-case exposure. Order IDs must
/// increase monotonically within a session, preventing reuse without an
/// unbounded historical set. `settle` converts a reservation to its filled
/// quantity when an order is terminal. Account and reservation lookups use
/// fixed-capacity open-addressed indices at load <= 1/2; reservations live in
/// stable slots drawn from a bounded free-list.
#[derive(Debug)]
pub struct RiskEngine<const ACCOUNTS: usize, const ORDERS: usize> {
    accounts: [Option<AccountState>; ACCOUNTS],
    account_index: ProbeIndex<ACCOUNTS, INDEX_PLANES>,
    reservations: [ReservationSlot; ORDERS],
    reservation_free_head: usize,
    reservation_index: ProbeIndex<ORDERS, INDEX_PLANES>,
    maximum_order_id: Option<OrderId>,
    killed: bool,
}

impl<const ACCOUNTS: usize, const ORDERS: usize> RiskEngine<ACCOUNTS, ORDERS> {
    #[must_use]
    pub const fn new() -> Self {
        let mut reservations = [ReservationSlot::Free { next_free: NIL }; ORDERS];
        let mut index = 0;
        while index < ORDERS {
            reservations[index] = ReservationSlot::Free {
                next_free: if index + 1 < ORDERS { index + 1 } else { NIL },
            };
            index += 1;
        }
        Self {
            accounts: [None; ACCOUNTS],
            account_index: ProbeIndex::new(),
            reservations,
            reservation_free_head: if ORDERS == 0 { NIL } else { 0 },
            reservation_index: ProbeIndex::new(),
            maximum_order_id: None,
            killed: false,
        }
    }

    #[must_use]
    pub fn export_state(&self) -> RiskEngineState {
        let mut accounts: Vec<_> = self
            .accounts
            .iter()
            .flatten()
            .map(|account| AccountRiskState {
                id: account.id,
                limits: account.limits,
                settled_position: account.settled_position,
                reserved_buys: account.reserved_buys,
                reserved_sells: account.reserved_sells,
                open_orders: account.open_orders,
                killed: account.killed,
            })
            .collect();
        accounts.sort_unstable_by_key(|account| account.id);

        let mut reservations: Vec<_> = self
            .reservations
            .iter()
            .filter_map(|slot| match slot {
                ReservationSlot::Live(reservation) => Some(ReservationRiskState {
                    order_id: reservation.order_id,
                    account_id: reservation.account_id,
                    side: reservation.side,
                    quantity: reservation.quantity,
                }),
                ReservationSlot::Free { .. } => None,
            })
            .collect();
        reservations.sort_unstable_by_key(|reservation| reservation.order_id);

        RiskEngineState {
            accounts,
            reservations,
            maximum_order_id: self.maximum_order_id,
            killed: self.killed,
        }
    }

    /// Rebuilds risk state from logical records without retaining storage
    /// slots or index positions from the source engine.
    ///
    /// # Errors
    ///
    /// Returns the first capacity, identity, limit, ownership, watermark, or
    /// exposure error. No engine is returned unless the complete state is valid.
    pub fn from_state(state: &RiskEngineState) -> Result<Self, RiskStateError> {
        let (accounts, reservations) = validate_state::<ACCOUNTS, ORDERS>(state)?;
        let mut engine = Self::new();
        engine.killed = state.killed;
        engine.maximum_order_id = state.maximum_order_id;
        for (slot, account) in accounts.into_iter().enumerate() {
            engine
                .account_index
                .insert(u64::from(account.id.0), slot)
                .map_err(|error| match error {
                    ProbeError::Full => RiskStateError::AccountCapacity,
                    ProbeError::Duplicate => RiskStateError::DuplicateAccount,
                })?;
            engine.accounts[slot] = Some(AccountState {
                id: account.id,
                limits: account.limits,
                settled_position: account.settled_position,
                reserved_buys: account.reserved_buys,
                reserved_sells: account.reserved_sells,
                open_orders: account.open_orders,
                killed: account.killed,
            });
        }
        for (slot, reservation) in reservations.into_iter().enumerate() {
            let index_slot = engine
                .reservation_index
                .insert(reservation.order_id.0, slot)
                .map_err(|error| match error {
                    ProbeError::Full => RiskStateError::ReservationCapacity,
                    ProbeError::Duplicate => RiskStateError::DuplicateOrder,
                })?;
            engine.reservations[slot] = ReservationSlot::Live(Reservation {
                order_id: reservation.order_id,
                account_id: reservation.account_id,
                index_slot,
                side: reservation.side,
                quantity: reservation.quantity,
            });
        }
        engine.reservation_free_head = if state.reservations.len() == ORDERS {
            NIL
        } else {
            state.reservations.len()
        };
        for slot in state.reservations.len()..ORDERS {
            engine.reservations[slot] = ReservationSlot::Free {
                next_free: if slot + 1 < ORDERS { slot + 1 } else { NIL },
            };
        }
        Ok(engine)
    }

    fn account_slot(&self, id: AccountId) -> Option<usize> {
        self.account_index.lookup(u64::from(id.0))
    }

    fn live_reservation(&self, order_id: OrderId) -> Option<(usize, Reservation)> {
        let slot = self.reservation_index.lookup(order_id.0)?;
        let Some(ReservationSlot::Live(reservation)) = self.reservations.get(slot).copied() else {
            return None;
        };
        if reservation.order_id != order_id {
            return None;
        }
        Some((slot, reservation))
    }
    /// Removes a live reservation from the index and returns its slot to the
    /// free list. Infallible for a reservation resolved by `live_reservation`.
    fn release_reservation(&mut self, slot: usize, reservation: Reservation) {
        let reservations = &mut self.reservations;
        let removed = self.reservation_index.remove_at(
            reservation.index_slot,
            reservation.order_id.0,
            |moved_key, moved_slot, new_flat| {
                let Some(ReservationSlot::Live(moved)) = reservations.get_mut(moved_slot) else {
                    return false;
                };
                if moved.order_id.0 != moved_key {
                    return false;
                }
                moved.index_slot = new_flat;
                true
            },
        );
        debug_assert_eq!(removed, Some(slot));
        self.reservations[slot] = ReservationSlot::Free {
            next_free: self.reservation_free_head,
        };
        self.reservation_free_head = slot;
    }

    /// # Errors
    ///
    /// Returns a duplicate, invalid-limits, or account-capacity error.
    pub fn register_account(
        &mut self,
        id: AccountId,
        limits: RiskLimits,
    ) -> Result<(), RegistrationError> {
        if limits.max_quantity.0 == 0
            || limits.max_open_orders == 0
            || limits.minimum_price.0 > limits.maximum_price.0
        {
            return Err(RegistrationError::InvalidLimits);
        }
        if self.account_slot(id).is_some() {
            return Err(RegistrationError::DuplicateAccount);
        }
        let Some(slot) = self.accounts.iter().position(Option::is_none) else {
            return Err(RegistrationError::AccountCapacity);
        };
        match self.account_index.insert(u64::from(id.0), slot) {
            Ok(_) => {}
            Err(ProbeError::Full) => return Err(RegistrationError::AccountCapacity),
            Err(ProbeError::Duplicate) => return Err(RegistrationError::DuplicateAccount),
        }
        self.accounts[slot] = Some(AccountState {
            id,
            limits,
            settled_position: 0,
            reserved_buys: 0,
            reserved_sells: 0,
            open_orders: 0,
            killed: false,
        });
        Ok(())
    }

    pub const fn set_kill_switch(&mut self, killed: bool) {
        self.killed = killed;
    }

    /// # Errors
    ///
    /// Returns [`RejectReason::UnknownAccount`] for an unregistered account.
    pub fn set_account_kill_switch(
        &mut self,
        account_id: AccountId,
        killed: bool,
    ) -> Result<(), RejectReason> {
        let slot = self
            .account_slot(account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let account = self.accounts[slot]
            .as_mut()
            .ok_or(RejectReason::UnknownAccount)?;
        account.killed = killed;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns the first deterministic limit, arithmetic, duplicate, kill, or
    /// fixed-capacity rejection. Rejection does not mutate risk state.
    pub fn check_and_reserve(&mut self, order: NewOrder) -> Result<(), RejectReason> {
        if self.killed {
            return Err(RejectReason::KillSwitch);
        }
        if order.quantity.0 == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if self
            .maximum_order_id
            .is_some_and(|maximum| order.order_id <= maximum)
        {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.reservation_free_head == NIL {
            return Err(RejectReason::OrderCapacity);
        }
        let account_slot = self
            .account_slot(order.account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let account = self.accounts[account_slot].ok_or(RejectReason::UnknownAccount)?;
        let (reserved_buys, reserved_sells, open_orders) = evaluate_limits(&account, &order)?;

        let reservation_slot = self.reservation_free_head;
        let Some(ReservationSlot::Free { next_free }) =
            self.reservations.get(reservation_slot).copied()
        else {
            return Err(RejectReason::ArithmeticOverflow);
        };
        let index_slot = match self
            .reservation_index
            .insert(order.order_id.0, reservation_slot)
        {
            Ok(index_slot) => index_slot,
            Err(ProbeError::Full) => return Err(RejectReason::OrderCapacity),
            Err(ProbeError::Duplicate) => return Err(RejectReason::ArithmeticOverflow),
        };
        self.accounts[account_slot] = Some(AccountState {
            reserved_buys,
            reserved_sells,
            open_orders,
            ..account
        });
        self.reservation_free_head = next_free;
        self.reservations[reservation_slot] = ReservationSlot::Live(Reservation {
            order_id: order.order_id,
            account_id: order.account_id,
            index_slot,
            side: order.side,
            quantity: order.quantity,
        });
        self.maximum_order_id = Some(order.order_id);
        Ok(())
    }

    /// Releases unfilled exposure and retains filled exposure as position.
    ///
    /// # Errors
    ///
    /// Returns an explicit state or arithmetic rejection for an unknown order
    /// or an invalid filled quantity. Rejection does not mutate state.
    pub fn settle(
        &mut self,
        order_id: OrderId,
        filled_quantity: Quantity,
    ) -> Result<(), RejectReason> {
        let Some((reservation_slot, reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        if filled_quantity.0 > reservation.quantity.0 {
            return Err(RejectReason::InvalidQuantity);
        }
        self.settle_reservation(reservation_slot, reservation, filled_quantity)
    }

    /// Settles a located reservation: the filled quantity becomes settled
    /// position and the full remaining reservation is released.
    fn settle_reservation(
        &mut self,
        reservation_slot: usize,
        reservation: Reservation,
        filled_quantity: Quantity,
    ) -> Result<(), RejectReason> {
        let account_slot = self
            .account_slot(reservation.account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let account = self.accounts[account_slot].ok_or(RejectReason::UnknownAccount)?;
        let (settled_position, reserved_buys, reserved_sells) = exposure_after_fill(
            &account,
            reservation.side,
            filled_quantity.0,
            reservation.quantity.0,
        )?;
        let open_orders = account
            .open_orders
            .checked_sub(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        self.accounts[account_slot] = Some(AccountState {
            settled_position,
            reserved_buys,
            reserved_sells,
            open_orders,
            ..account
        });
        self.release_reservation(reservation_slot, reservation);
        Ok(())
    }

    /// Applies a fill while retaining the signed projected exposure. When the
    /// reservation is fully filled, the order is closed but the resulting
    /// position remains in the projected position.
    ///
    /// # Errors
    ///
    /// Returns an explicit state or quantity rejection when the reservation
    /// does not exist or the fill exceeds the reserved remainder.
    pub fn record_fill(
        &mut self,
        order_id: OrderId,
        filled_quantity: Quantity,
    ) -> Result<(), RejectReason> {
        if filled_quantity.0 == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        let Some((reservation_slot, mut reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        reservation.quantity.0 = reservation
            .quantity
            .0
            .checked_sub(filled_quantity.0)
            .ok_or(RejectReason::InvalidQuantity)?;
        let account_slot = self
            .account_slot(reservation.account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let account = self.accounts[account_slot].ok_or(RejectReason::UnknownAccount)?;
        let (settled_position, reserved_buys, reserved_sells) = exposure_after_fill(
            &account,
            reservation.side,
            filled_quantity.0,
            filled_quantity.0,
        )?;
        let open_orders = if reservation.quantity.0 == 0 {
            account
                .open_orders
                .checked_sub(1)
                .ok_or(RejectReason::ArithmeticOverflow)?
        } else {
            account.open_orders
        };
        self.accounts[account_slot] = Some(AccountState {
            settled_position,
            reserved_buys,
            reserved_sells,
            open_orders,
            ..account
        });
        if reservation.quantity.0 == 0 {
            self.release_reservation(reservation_slot, reservation);
        } else {
            self.reservations[reservation_slot] = ReservationSlot::Live(reservation);
        }
        Ok(())
    }

    /// Amends an owned reservation's total quantity without touching the
    /// book. Returns the released amount: zero for increases and no-op
    /// changes. Increases re-check position, notional, and arithmetic limits
    /// at the new total; decreases release exposure immediately.
    ///
    /// # Errors
    ///
    /// Returns unknown-order or ownership rejection, limit rejection for an
    /// unaffordable increase, or arithmetic failure. Rejection does not
    /// mutate state.
    pub fn adjust_reservation(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        price: PriceTicks,
        new_quantity: Quantity,
    ) -> Result<(Quantity, Quantity), RejectReason> {
        let Some((slot, mut reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        if reservation.account_id != account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        let remaining = reservation.quantity.0;
        let new_total = new_quantity.0;
        let prior = Quantity(remaining);
        if new_total == remaining {
            return Ok((Quantity(0), prior));
        }
        let account_slot = self
            .account_slot(account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let mut account = self.accounts[account_slot].ok_or(RejectReason::UnknownAccount)?;
        if new_total > remaining {
            let added = new_total - remaining;
            let (reserved_buys, reserved_sells) = match reservation.side {
                Side::Buy => (
                    account
                        .reserved_buys
                        .checked_add(u128::from(added))
                        .ok_or(RejectReason::ArithmeticOverflow)?,
                    account.reserved_sells,
                ),
                Side::Sell => (
                    account.reserved_buys,
                    account
                        .reserved_sells
                        .checked_add(u128::from(added))
                        .ok_or(RejectReason::ArithmeticOverflow)?,
                ),
            };
            let absolute_price = price
                .0
                .checked_abs()
                .ok_or(RejectReason::ArithmeticOverflow)?;
            let notional = u128::from(
                u64::try_from(absolute_price).map_err(|_| RejectReason::ArithmeticOverflow)?,
            )
            .checked_mul(u128::from(new_total))
            .ok_or(RejectReason::ArithmeticOverflow)?;
            if notional > account.limits.max_notional {
                return Err(RejectReason::NotionalLimit);
            }
            let maximum_position = i128::from(account.limits.max_abs_position.0);
            let worst_long = account
                .settled_position
                .checked_add(
                    i128::try_from(reserved_buys).map_err(|_| RejectReason::ArithmeticOverflow)?,
                )
                .ok_or(RejectReason::ArithmeticOverflow)?;
            let worst_short = account
                .settled_position
                .checked_sub(
                    i128::try_from(reserved_sells).map_err(|_| RejectReason::ArithmeticOverflow)?,
                )
                .ok_or(RejectReason::ArithmeticOverflow)?;
            if worst_long > maximum_position || worst_short < -maximum_position {
                return Err(RejectReason::PositionLimit);
            }
            match reservation.side {
                Side::Buy => account.reserved_buys = reserved_buys,
                Side::Sell => account.reserved_sells = reserved_sells,
            }
            reservation.quantity = Quantity(new_total);
            self.accounts[account_slot] = Some(account);
            self.reservations[slot] = ReservationSlot::Live(reservation);
            Ok((Quantity(0), prior))
        } else {
            let released = remaining - new_total;
            match reservation.side {
                Side::Buy => {
                    account.reserved_buys = account
                        .reserved_buys
                        .checked_sub(u128::from(released))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
                Side::Sell => {
                    account.reserved_sells = account
                        .reserved_sells
                        .checked_sub(u128::from(released))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
            }
            reservation.quantity = Quantity(new_total);
            self.accounts[account_slot] = Some(account);
            self.reservations[slot] = ReservationSlot::Live(reservation);
            Ok((Quantity(released), prior))
        }
    }

    /// Restores a reservation's total quantity unconditionally after a
    /// rolled-back book mutation. Skips limit checks: the prior state
    /// provably passed them, and the rollback must not fail.
    ///
    /// # Errors
    ///
    /// Returns unknown-order or ownership rejection if no live reservation
    /// matches.
    pub fn restore_reservation(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        prior_quantity: Quantity,
    ) -> Result<(), RejectReason> {
        let Some((slot, mut reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        if reservation.account_id != account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        let current = reservation.quantity.0;
        let prior = prior_quantity.0;
        if current == prior {
            return Ok(());
        }
        let account_slot = self
            .account_slot(account_id)
            .ok_or(RejectReason::UnknownAccount)?;
        let mut account = self.accounts[account_slot].ok_or(RejectReason::UnknownAccount)?;
        if prior > current {
            match reservation.side {
                Side::Buy => {
                    account.reserved_buys = account
                        .reserved_buys
                        .checked_add(u128::from(prior - current))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
                Side::Sell => {
                    account.reserved_sells = account
                        .reserved_sells
                        .checked_add(u128::from(prior - current))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
            }
        } else {
            let released = current - prior;
            match reservation.side {
                Side::Buy => {
                    account.reserved_buys = account
                        .reserved_buys
                        .checked_sub(u128::from(released))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
                Side::Sell => {
                    account.reserved_sells = account
                        .reserved_sells
                        .checked_sub(u128::from(released))
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
            }
        }
        reservation.quantity = Quantity(prior);
        self.accounts[account_slot] = Some(account);
        self.reservations[slot] = ReservationSlot::Live(reservation);
        Ok(())
    }
    /// Validates ownership of an open reservation without mutation.
    ///
    /// # Errors
    ///
    /// Returns unknown-order or ownership rejection.
    pub fn can_cancel(&self, order_id: OrderId, account_id: AccountId) -> Result<(), RejectReason> {
        let Some((_, reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        if reservation.account_id != account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        Ok(())
    }

    /// Releases the full remaining reservation for an owned canceled order.
    ///
    /// # Errors
    ///
    /// Returns unknown-order, ownership, or internal arithmetic rejection.
    pub fn cancel_reservation(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
    ) -> Result<Quantity, RejectReason> {
        let Some((reservation_slot, reservation)) = self.live_reservation(order_id) else {
            return Err(RejectReason::UnknownOrder);
        };
        if reservation.account_id != account_id {
            return Err(RejectReason::NotOrderOwner);
        }
        let remaining = reservation.quantity;
        self.settle_reservation(reservation_slot, reservation, Quantity(0))?;
        Ok(remaining)
    }

    #[must_use]
    pub fn stable_digest(&self) -> u64 {
        let mut digest = 0xcbf2_9ce4_8422_2325_u64;
        mix(&mut digest, u64::from(self.killed));
        mix(
            &mut digest,
            self.maximum_order_id.map_or(0, |order_id| order_id.0),
        );
        for account in self.accounts.iter().flatten() {
            mix(&mut digest, u64::from(account.id.0));
            // 128-bit values mix as canonical big-endian high/low lanes so the
            // digest is identical across CPU byte orders.
            let position = account.settled_position.to_be_bytes();
            mix(&mut digest, be_high(&position));
            for exposure in [account.reserved_buys, account.reserved_sells] {
                let bytes = exposure.to_be_bytes();
                mix(&mut digest, be_high(&bytes));
                mix(&mut digest, be_low(&bytes));
            }
            mix(&mut digest, be_low(&position));
            mix(&mut digest, u64::from(account.open_orders));
            mix(&mut digest, u64::from(account.killed));
        }
        for slot in &self.reservations {
            if let ReservationSlot::Live(reservation) = slot {
                mix(&mut digest, reservation.order_id.0);
                mix(&mut digest, reservation.quantity.0);
            }
        }
        digest
    }

    #[must_use]
    pub fn account_snapshot(&self, id: AccountId) -> Option<(i128, u32)> {
        let slot = self.account_slot(id)?;
        self.accounts[slot].and_then(|account| {
            let buys = i128::try_from(account.reserved_buys).ok()?;
            let sells = i128::try_from(account.reserved_sells).ok()?;
            let projected = account
                .settled_position
                .checked_add(buys)?
                .checked_sub(sells)?;
            Some((projected, account.open_orders))
        })
    }
}

fn validate_state<const ACCOUNTS: usize, const ORDERS: usize>(
    state: &RiskEngineState,
) -> Result<(Vec<AccountRiskState>, Vec<ReservationRiskState>), RiskStateError> {
    if state.accounts.len() > ACCOUNTS {
        return Err(RiskStateError::AccountCapacity);
    }
    if state.reservations.len() > ORDERS {
        return Err(RiskStateError::ReservationCapacity);
    }
    let accounts = state.accounts.clone();
    for pair in accounts.windows(2) {
        if pair[0].id == pair[1].id {
            return Err(RiskStateError::DuplicateAccount);
        }
        if pair[0].id > pair[1].id {
            return Err(RiskStateError::NonCanonical);
        }
    }
    if accounts.iter().any(|account| {
        account.limits.max_quantity.0 == 0
            || account.limits.max_open_orders == 0
            || account.limits.minimum_price.0 > account.limits.maximum_price.0
    }) {
        return Err(RiskStateError::InvalidLimits);
    }
    let reservations = state.reservations.clone();
    for pair in reservations.windows(2) {
        if pair[0].order_id == pair[1].order_id {
            return Err(RiskStateError::DuplicateOrder);
        }
        if pair[0].order_id > pair[1].order_id {
            return Err(RiskStateError::NonCanonical);
        }
    }
    if reservations
        .iter()
        .any(|reservation| reservation.quantity.0 == 0)
    {
        return Err(RiskStateError::InvalidQuantity);
    }
    if reservations.last().is_some_and(|reservation| {
        state
            .maximum_order_id
            .is_none_or(|maximum| maximum < reservation.order_id)
    }) {
        return Err(RiskStateError::InvalidWatermark);
    }
    if reservations.iter().any(|reservation| {
        accounts
            .binary_search_by_key(&reservation.account_id, |account| account.id)
            .is_err()
    }) {
        return Err(RiskStateError::UnknownAccount);
    }
    for account in &accounts {
        validate_account_state(account, &reservations)?;
    }
    Ok((accounts, reservations))
}

fn validate_account_state(
    account: &AccountRiskState,
    reservations: &[ReservationRiskState],
) -> Result<(), RiskStateError> {
    let mut reserved_buys = 0_u128;
    let mut reserved_sells = 0_u128;
    let mut open_orders = 0_u32;
    for reservation in reservations
        .iter()
        .filter(|reservation| reservation.account_id == account.id)
    {
        match reservation.side {
            Side::Buy => {
                reserved_buys = reserved_buys
                    .checked_add(u128::from(reservation.quantity.0))
                    .ok_or(RiskStateError::ArithmeticOverflow)?;
            }
            Side::Sell => {
                reserved_sells = reserved_sells
                    .checked_add(u128::from(reservation.quantity.0))
                    .ok_or(RiskStateError::ArithmeticOverflow)?;
            }
        }
        open_orders = open_orders
            .checked_add(1)
            .ok_or(RiskStateError::ArithmeticOverflow)?;
    }
    if reserved_buys != account.reserved_buys || reserved_sells != account.reserved_sells {
        return Err(RiskStateError::ExposureMismatch);
    }
    if open_orders != account.open_orders || open_orders > account.limits.max_open_orders {
        return Err(RiskStateError::OpenOrderMismatch);
    }
    let buys = i128::try_from(reserved_buys).map_err(|_| RiskStateError::ArithmeticOverflow)?;
    let sells = i128::try_from(reserved_sells).map_err(|_| RiskStateError::ArithmeticOverflow)?;
    let worst_long = account
        .settled_position
        .checked_add(buys)
        .ok_or(RiskStateError::ArithmeticOverflow)?;
    let worst_short = account
        .settled_position
        .checked_sub(sells)
        .ok_or(RiskStateError::ArithmeticOverflow)?;
    let maximum = i128::from(account.limits.max_abs_position.0);
    if worst_long > maximum || worst_short < -maximum {
        return Err(RiskStateError::PositionLimit);
    }
    Ok(())
}

impl<const ACCOUNTS: usize, const ORDERS: usize> Default for RiskEngine<ACCOUNTS, ORDERS> {
    fn default() -> Self {
        Self::new()
    }
}

/// Post-fill account totals for one reservation side: `filled` becomes
/// settled position and `released` leaves the reserved total.
fn exposure_after_fill(
    account: &AccountState,
    side: Side,
    filled: u64,
    released: u64,
) -> Result<(i128, u128, u128), RejectReason> {
    let settled_position = match side {
        Side::Buy => account.settled_position.checked_add(i128::from(filled)),
        Side::Sell => account.settled_position.checked_sub(i128::from(filled)),
    }
    .ok_or(RejectReason::ArithmeticOverflow)?;
    let (reserved_buys, reserved_sells) = match side {
        Side::Buy => (
            account
                .reserved_buys
                .checked_sub(u128::from(released))
                .ok_or(RejectReason::ArithmeticOverflow)?,
            account.reserved_sells,
        ),
        Side::Sell => (
            account.reserved_buys,
            account
                .reserved_sells
                .checked_sub(u128::from(released))
                .ok_or(RejectReason::ArithmeticOverflow)?,
        ),
    };
    Ok((settled_position, reserved_buys, reserved_sells))
}

/// Pure limit evaluation for one order against one account. Returns the
/// post-reservation totals without mutating anything.
fn evaluate_limits(
    account: &AccountState,
    order: &NewOrder,
) -> Result<(u128, u128, u32), RejectReason> {
    if account.killed {
        return Err(RejectReason::KillSwitch);
    }
    if order.quantity.0 > account.limits.max_quantity.0 {
        return Err(RejectReason::QuantityLimit);
    }
    if order.price.0 < account.limits.minimum_price.0
        || order.price.0 > account.limits.maximum_price.0
    {
        return Err(RejectReason::PriceCollar);
    }
    let absolute_price = order
        .price
        .0
        .checked_abs()
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let notional =
        u128::from(u64::try_from(absolute_price).map_err(|_| RejectReason::ArithmeticOverflow)?)
            .checked_mul(u128::from(order.quantity.0))
            .ok_or(RejectReason::ArithmeticOverflow)?;
    if notional > account.limits.max_notional {
        return Err(RejectReason::NotionalLimit);
    }
    let (reserved_buys, reserved_sells) = match order.side {
        Side::Buy => (
            account
                .reserved_buys
                .checked_add(u128::from(order.quantity.0))
                .ok_or(RejectReason::ArithmeticOverflow)?,
            account.reserved_sells,
        ),
        Side::Sell => (
            account.reserved_buys,
            account
                .reserved_sells
                .checked_add(u128::from(order.quantity.0))
                .ok_or(RejectReason::ArithmeticOverflow)?,
        ),
    };
    let maximum_position = i128::from(account.limits.max_abs_position.0);
    let worst_long = account
        .settled_position
        .checked_add(i128::try_from(reserved_buys).map_err(|_| RejectReason::ArithmeticOverflow)?)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let worst_short = account
        .settled_position
        .checked_sub(i128::try_from(reserved_sells).map_err(|_| RejectReason::ArithmeticOverflow)?)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    if worst_long > maximum_position || worst_short < -maximum_position {
        return Err(RejectReason::PositionLimit);
    }
    let open_orders = account
        .open_orders
        .checked_add(1)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    if open_orders > account.limits.max_open_orders {
        return Err(RejectReason::OpenOrderLimit);
    }
    Ok((reserved_buys, reserved_sells, open_orders))
}

fn mix(digest: &mut u64, value: u64) {
    *digest ^= value;
    *digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
}

fn be_high(bytes: &[u8; 16]) -> u64 {
    u64::from_be_bytes(bytes[..8].try_into().expect("eight high bytes"))
}

fn be_low(bytes: &[u8; 16]) -> u64 {
    u64::from_be_bytes(bytes[8..].try_into().expect("eight low bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_types::{InstrumentId, SequenceNumber};

    fn limits() -> RiskLimits {
        RiskLimits {
            max_quantity: Quantity(10),
            max_notional: 1_000,
            max_abs_position: Quantity(12),
            max_open_orders: 2,
            minimum_price: PriceTicks(1),
            maximum_price: PriceTicks(100),
        }
    }

    fn order(id: u64, side: Side, quantity: u64) -> NewOrder {
        NewOrder {
            time_in_force: hft_types::TimeInForce::Gtc,
            order_id: OrderId(id),
            account_id: AccountId(7),
            instrument_id: InstrumentId(1),
            price: PriceTicks(10),
            quantity: Quantity(quantity),
            sequence: SequenceNumber(id),
            side,
        }
    }

    fn wide_limits() -> RiskLimits {
        RiskLimits {
            max_quantity: Quantity(1_000),
            max_notional: 1_000_000_000,
            max_abs_position: Quantity(1_000),
            max_open_orders: 64,
            minimum_price: PriceTicks(1),
            maximum_price: PriceTicks(1_000),
        }
    }

    /// Index and storage agree, live/free sets are disjoint, and reservation
    /// totals equal live per-account exposure.
    fn assert_risk_consistent<const ACCOUNTS: usize, const ORDERS: usize>(
        risk: &RiskEngine<ACCOUNTS, ORDERS>,
    ) {
        let mut live_totals: std::collections::BTreeMap<u32, (u128, u128, u32)> =
            std::collections::BTreeMap::new();
        let mut live = 0_usize;
        for (slot_index, slot) in risk.reservations.iter().enumerate() {
            let ReservationSlot::Live(reservation) = slot else {
                continue;
            };
            assert_eq!(
                risk.reservation_index.lookup(reservation.order_id.0),
                Some(slot_index),
                "index maps the id to its stable slot"
            );
            let flat = usize::try_from(reservation.index_slot).expect("valid index slot");
            assert_eq!(
                risk.reservation_index.slot(flat),
                &IndexSlot::Occupied {
                    key: reservation.order_id.0,
                    value: NonZeroUsize::new(slot_index + 1).expect("live slot is representable"),
                },
                "index slot points back at the reservation"
            );
            let totals = live_totals.entry(reservation.account_id.0).or_default();
            match reservation.side {
                Side::Buy => totals.0 += u128::from(reservation.quantity.0),
                Side::Sell => totals.1 += u128::from(reservation.quantity.0),
            }
            totals.2 += 1;
            live += 1;
        }
        let mut free_seen = 0_usize;
        let mut cursor = risk.reservation_free_head;
        while cursor != NIL {
            let ReservationSlot::Free { next_free } =
                risk.reservations.get(cursor).expect("free slot in range")
            else {
                panic!("free chain reached a live slot");
            };
            cursor = *next_free;
            free_seen += 1;
        }
        assert_eq!(live + free_seen, ORDERS, "live and free sets cover slots");
        for (position, entry) in risk.accounts.iter().enumerate() {
            let Some(account) = entry else {
                continue;
            };
            assert_eq!(
                risk.account_index.lookup(u64::from(account.id.0)),
                Some(position),
                "account index maps to the registration slot"
            );
            let (buys, sells, open) = live_totals.get(&account.id.0).copied().unwrap_or_default();
            assert_eq!(
                account.reserved_buys, buys,
                "reserved buys equal live exposure"
            );
            assert_eq!(
                account.reserved_sells, sells,
                "reserved sells equal live exposure"
            );
            assert_eq!(account.open_orders, open, "open orders equal live count");
        }
    }

    #[test]
    fn index_slots_use_the_handle_niche_without_extra_storage() {
        assert_eq!(
            core::mem::size_of::<IndexSlot>(),
            core::mem::size_of::<(u64, usize)>(),
        );
        assert!(core::mem::size_of::<IndexSlot>() < core::mem::size_of::<Option<(u64, usize)>>());
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(core::mem::size_of::<IndexSlot>(), 16);
            assert_eq!(core::mem::size_of::<Option<(u64, usize)>>(), 24);
        }
    }

    #[test]
    fn compact_handles_preserve_boundary_keys_and_back_shift_values() {
        let mut index = ProbeIndex::<4, 2>::new();
        let first = index.insert(0, 0).expect("first handle");
        let second = index.insert(8, 3).expect("last storage handle");
        let maximum = index
            .insert(u64::MAX, usize::MAX - 1)
            .expect("maximum encodable handle");
        index
            .insert(u64::MAX - 8, usize::MAX - 2)
            .expect("colliding high handle");
        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(8), Some(3));
        assert_eq!(index.lookup(u64::MAX), Some(usize::MAX - 1));
        assert_eq!(index.lookup(u64::MAX - 8), Some(usize::MAX - 2));

        let before = index.slots;
        assert_eq!(index.insert(1, usize::MAX), Err(ProbeError::Full));
        assert_eq!(index.insert(0, 0), Err(ProbeError::Duplicate));
        assert_eq!(index.slots, before, "rejected inserts do not mutate");

        let mut moved = None;
        assert_eq!(
            index.remove_at(first, 0, |key, value, flat| {
                moved = Some((key, value, flat));
                true
            }),
            Some(0),
        );
        assert_eq!(moved, Some((8, 3, first)));
        assert_eq!(index.lookup(0), None);
        assert_eq!(index.lookup(8), Some(3));
        assert_eq!(
            index.remove_at(maximum, u64::MAX, |key, value, flat| {
                moved = Some((key, value, flat));
                true
            }),
            Some(usize::MAX - 1),
        );
        assert_eq!(moved, Some((u64::MAX - 8, usize::MAX - 2, maximum)));
        assert_eq!(index.lookup(u64::MAX), None);
        assert_eq!(index.lookup(u64::MAX - 8), Some(usize::MAX - 2));
        assert_eq!(index.insert(0, 0), Ok(second), "empty slots can be reused");
    }

    #[test]
    fn checks_limits_without_mutating_on_reject() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), limits())
            .expect("valid limits");
        assert_eq!(
            risk.check_and_reserve(order(1, Side::Buy, 11)),
            Err(RejectReason::QuantityLimit)
        );
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((0, 0)));
        assert_eq!(risk.check_and_reserve(order(1, Side::Buy, 8)), Ok(()));
        assert_eq!(
            risk.check_and_reserve(order(1, Side::Sell, 1)),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((8, 1)));
    }

    #[test]
    fn enforces_projected_position_and_settles_unfilled() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), limits())
            .expect("valid limits");
        assert_eq!(risk.check_and_reserve(order(1, Side::Buy, 8)), Ok(()));
        assert_eq!(
            risk.check_and_reserve(order(2, Side::Buy, 5)),
            Err(RejectReason::PositionLimit)
        );
        assert_eq!(risk.settle(OrderId(1), Quantity(3)), Ok(()));
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((3, 0)));
    }

    #[test]
    fn deterministic_sequence_has_stable_digest() {
        let mut first = RiskEngine::<1, 4>::new();
        let mut second = RiskEngine::<1, 4>::new();
        for engine in [&mut first, &mut second] {
            engine
                .register_account(AccountId(7), limits())
                .expect("valid limits");
            engine
                .check_and_reserve(order(1, Side::Buy, 2))
                .expect("within limits");
            engine
                .check_and_reserve(order(2, Side::Sell, 1))
                .expect("within limits");
        }
        assert_eq!(first.stable_digest(), second.stable_digest());
    }

    #[test]
    fn fills_close_order_without_releasing_position() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), limits())
            .expect("valid limits");
        risk.check_and_reserve(order(1, Side::Buy, 5))
            .expect("within limits");
        risk.record_fill(OrderId(1), Quantity(2))
            .expect("partial fill");
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((5, 1)));
        risk.record_fill(OrderId(1), Quantity(3))
            .expect("terminal fill");
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((5, 0)));
    }

    #[test]
    fn completed_and_out_of_order_ids_are_not_reusable() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), limits())
            .expect("valid limits");
        risk.check_and_reserve(order(10, Side::Buy, 1))
            .expect("first order");
        risk.record_fill(OrderId(10), Quantity(1))
            .expect("terminal fill");
        assert_eq!(
            risk.check_and_reserve(order(10, Side::Buy, 1)),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(
            risk.check_and_reserve(order(9, Side::Buy, 1)),
            Err(RejectReason::DuplicateOrderId)
        );
    }

    #[test]
    fn opposing_reservations_do_not_cancel_worst_case_exposure() {
        let mut risk = RiskEngine::<1, 4>::new();
        risk.register_account(AccountId(7), limits())
            .expect("valid limits");
        risk.check_and_reserve(order(1, Side::Buy, 10))
            .expect("buy within limit");
        risk.check_and_reserve(order(2, Side::Sell, 10))
            .expect("sell within limit");
        assert_eq!(
            risk.check_and_reserve(order(3, Side::Buy, 3)),
            Err(RejectReason::PositionLimit)
        );
    }

    #[test]
    fn cancel_releases_only_owned_remaining_reservation() {
        let mut risk = RiskEngine::<2, 4>::new();
        risk.register_account(AccountId(7), limits())
            .expect("first account");
        risk.register_account(AccountId(8), limits())
            .expect("second account");
        risk.check_and_reserve(order(1, Side::Buy, 5))
            .expect("reserve order");
        risk.record_fill(OrderId(1), Quantity(2))
            .expect("partial fill");
        assert_eq!(
            risk.cancel_reservation(OrderId(1), AccountId(8)),
            Err(RejectReason::NotOrderOwner)
        );
        assert_eq!(
            risk.cancel_reservation(OrderId(1), AccountId(7)),
            Ok(Quantity(3))
        );
        assert_eq!(risk.account_snapshot(AccountId(7)), Some((2, 0)));
    }

    #[test]
    fn index_handles_collisions_back_shift_and_slot_reuse() {
        type TestIndex = ProbeIndex<4, INDEX_PLANES>;
        let home = TestIndex::probe_start(1);
        let mut colliding = std::vec::Vec::new();
        let mut candidate = 1_u64;
        while colliding.len() < 4 {
            if TestIndex::probe_start(candidate) == home {
                colliding.push(candidate);
            }
            candidate += 1;
        }
        let mut risk = RiskEngine::<1, 4>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("valid limits");
        for id in &colliding[..3] {
            risk.check_and_reserve(order(*id, Side::Buy, 2))
                .expect("reserve colliding order");
            assert_risk_consistent(&risk);
        }
        risk.settle(OrderId(colliding[1]), Quantity(0))
            .expect("settle middle colliding order");
        assert_risk_consistent(&risk);
        risk.cancel_reservation(OrderId(colliding[0]), AccountId(7))
            .expect("cancel relocated order");
        assert_risk_consistent(&risk);
        risk.check_and_reserve(order(colliding[3], Side::Sell, 1))
            .expect("reuse freed slot");
        assert_risk_consistent(&risk);
        assert_eq!(
            risk.can_cancel(OrderId(colliding[2]), AccountId(7)),
            Ok(()),
            "lookup after deletions and reuse"
        );
    }

    #[test]
    fn full_reservations_reject_atomically() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("valid limits");
        risk.check_and_reserve(order(1, Side::Buy, 1))
            .expect("first reservation");
        risk.check_and_reserve(order(2, Side::Sell, 1))
            .expect("second reservation");
        let digest = risk.stable_digest();
        assert_eq!(
            risk.check_and_reserve(order(3, Side::Buy, 1)),
            Err(RejectReason::OrderCapacity)
        );
        assert_eq!(risk.stable_digest(), digest);
        assert_risk_consistent(&risk);
    }

    #[test]
    fn duplicate_and_capacity_registration_reject() {
        let mut risk = RiskEngine::<1, 2>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("first registration");
        assert_eq!(
            risk.register_account(AccountId(7), wide_limits()),
            Err(RegistrationError::DuplicateAccount)
        );
        assert_eq!(
            risk.register_account(AccountId(8), wide_limits()),
            Err(RegistrationError::AccountCapacity)
        );
        assert_risk_consistent(&risk);
    }

    #[test]
    fn stable_handles_survive_reservation_churn() {
        let mut risk = RiskEngine::<1, 8>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("valid limits");
        for id in 1..=5 {
            risk.check_and_reserve(order(id, Side::Buy, 2))
                .expect("reserve order");
        }
        let before = risk.reservation_index.lookup(4);
        assert!(before.is_some());

        risk.record_fill(OrderId(1), Quantity(2))
            .expect("terminal fill");
        assert_risk_consistent(&risk);
        risk.cancel_reservation(OrderId(2), AccountId(7))
            .expect("cancel order");
        assert_risk_consistent(&risk);
        risk.check_and_reserve(order(6, Side::Sell, 1))
            .expect("reserve after frees");
        risk.settle(OrderId(3), Quantity(1))
            .expect("settle with partial fill");
        assert_risk_consistent(&risk);

        assert_eq!(
            risk.reservation_index.lookup(4),
            before,
            "unrelated churn preserves the stable handle"
        );
        risk.settle(OrderId(4), Quantity(0))
            .expect("settle through preserved handle");
        assert_risk_consistent(&risk);
    }

    #[test]
    fn shifted_reservations_stay_releasable_after_back_shift() {
        let mut risk = RiskEngine::<4, 64>::new();
        let limits = RiskLimits {
            max_quantity: Quantity(10),
            max_notional: 1_000_000,
            max_abs_position: Quantity(1_000),
            max_open_orders: 32,
            minimum_price: PriceTicks(1),
            maximum_price: PriceTicks(1_000),
        };
        for account in 1..=4_u32 {
            risk.register_account(AccountId(account), limits)
                .expect("register");
        }
        for id in 1..=40_u64 {
            let side = if id % 2 == 0 { Side::Sell } else { Side::Buy };
            risk.check_and_reserve(NewOrder {
                time_in_force: hft_types::TimeInForce::Gtc,
                order_id: OrderId(id),
                account_id: AccountId(u32::try_from((id % 4) + 1).expect("account")),
                instrument_id: InstrumentId(1),
                price: PriceTicks(100),
                quantity: Quantity(1),
                sequence: SequenceNumber(id),
                side,
            })
            .unwrap_or_else(|error| panic!("reserve {id}: {error:?}"));
        }
        // Interleaved releases force repeated back-shifts across the table;
        // every later release must still find its moved entry.
        for id in (1..=20).step_by(2) {
            risk.cancel_reservation(
                OrderId(id),
                AccountId(u32::try_from((id % 4) + 1).expect("account")),
            )
            .expect("cancel odd");
        }
        for id in (2..=20).step_by(2) {
            risk.cancel_reservation(
                OrderId(id),
                AccountId(u32::try_from((id % 4) + 1).expect("account")),
            )
            .expect("cancel even shifted");
        }
        for id in 21..=40 {
            risk.cancel_reservation(
                OrderId(id),
                AccountId(u32::try_from((id % 4) + 1).expect("account")),
            )
            .expect("cancel late shifted");
        }
        for account in 1..=4_u32 {
            assert_eq!(
                risk.account_snapshot(AccountId(account)),
                Some((0, 0)),
                "account {account} drained"
            );
        }
    }

    #[test]
    fn logical_state_round_trip_rebuilds_indexes() {
        let mut risk = RiskEngine::<4, 8>::new();
        risk.register_account(AccountId(9), wide_limits())
            .expect("register account 9");
        risk.register_account(AccountId(2), wide_limits())
            .expect("register account 2");
        let mut sell = order(7, Side::Sell, 4);
        sell.account_id = AccountId(2);
        risk.check_and_reserve(sell).expect("reserve sell");
        let mut buy = order(11, Side::Buy, 3);
        buy.account_id = AccountId(9);
        risk.check_and_reserve(buy).expect("reserve buy");
        risk.record_fill(OrderId(11), Quantity(1))
            .expect("partial fill");
        risk.set_account_kill_switch(AccountId(2), true)
            .expect("kill account");
        risk.set_kill_switch(true);

        let state = risk.export_state();
        assert_eq!(
            state
                .accounts
                .iter()
                .map(|account| account.id)
                .collect::<Vec<_>>(),
            [AccountId(2), AccountId(9)]
        );
        assert_eq!(
            state
                .reservations
                .iter()
                .map(|reservation| reservation.order_id)
                .collect::<Vec<_>>(),
            [OrderId(7), OrderId(11)]
        );

        let restored = RiskEngine::<4, 8>::from_state(&state).expect("restore state");
        assert_eq!(restored.export_state(), state);
        assert_risk_consistent(&restored);
    }

    #[test]
    fn logical_state_rejects_inconsistent_exposure_and_watermark() {
        let mut risk = RiskEngine::<2, 4>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("register");
        risk.check_and_reserve(order(3, Side::Buy, 2))
            .expect("reserve");
        let state = risk.export_state();

        let mut bad_exposure = state.clone();
        bad_exposure.accounts[0].reserved_buys += 1;
        assert_eq!(
            RiskEngine::<2, 4>::from_state(&bad_exposure).map(|_| ()),
            Err(RiskStateError::ExposureMismatch)
        );

        let mut bad_watermark = state;
        bad_watermark.maximum_order_id = Some(OrderId(2));
        assert_eq!(
            RiskEngine::<2, 4>::from_state(&bad_watermark).map(|_| ()),
            Err(RiskStateError::InvalidWatermark)
        );
    }

    #[test]
    fn logical_state_rejects_noncanonical_records_and_zero_quantity() {
        let mut risk = RiskEngine::<2, 4>::new();
        risk.register_account(AccountId(9), wide_limits())
            .expect("account nine");
        risk.register_account(AccountId(2), wide_limits())
            .expect("account two");
        let mut state = risk.export_state();
        state.accounts.swap(0, 1);
        assert_eq!(
            RiskEngine::<2, 4>::from_state(&state).map(|_| ()),
            Err(RiskStateError::NonCanonical)
        );

        let mut risk = RiskEngine::<2, 4>::new();
        risk.register_account(AccountId(7), wide_limits())
            .expect("account");
        risk.check_and_reserve(order(3, Side::Buy, 2))
            .expect("reservation");
        let mut state = risk.export_state();
        state.reservations[0].quantity = Quantity(0);
        state.accounts[0].reserved_buys = 0;
        assert_eq!(
            RiskEngine::<2, 4>::from_state(&state).map(|_| ()),
            Err(RiskStateError::InvalidQuantity)
        );
    }
}
