#![forbid(unsafe_code)]

use core::fmt;

macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }
    };
}

id_type!(OrderId, u64);
id_type!(AccountId, u32);
id_type!(InstrumentId, u32);
id_type!(PriceTicks, i64);
id_type!(Quantity, u64);
id_type!(SequenceNumber, u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Side {
    Buy = 1,
    Sell = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TimeInForce {
    Gtc = 1,
    Ioc = 2,
    Fok = 3,
    PostOnly = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub price: PriceTicks,
    pub quantity: Quantity,
    pub sequence: SequenceNumber,
    pub side: Side,
    pub time_in_force: TimeInForce,
}

/// Owned amend of a resting order: new price and new total quantity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct ReplaceOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub sequence: SequenceNumber,
    pub price: PriceTicks,
    pub quantity: Quantity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct CancelOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub sequence: SequenceNumber,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    NewOrder(NewOrder),
    CancelOrder(CancelOrder),
    ReplaceOrder(ReplaceOrder),
}

impl Command {
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        match self {
            Self::NewOrder(order) => order.instrument_id,
            Self::CancelOrder(cancel) => cancel.instrument_id,
            Self::ReplaceOrder(replace) => replace.instrument_id,
        }
    }

    #[must_use]
    pub const fn sequence(self) -> SequenceNumber {
        match self {
            Self::NewOrder(order) => order.sequence,
            Self::CancelOrder(cancel) => cancel.sequence,
            Self::ReplaceOrder(replace) => replace.sequence,
        }
    }
}

const _: () = assert!(core::mem::size_of::<CancelOrder>() == 24);
const _: () = assert!(core::mem::align_of::<CancelOrder>() == 8);

const _: () = assert!(core::mem::size_of::<NewOrder>() == 48);
const _: () = assert!(core::mem::align_of::<NewOrder>() == 8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderState {
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected(RejectReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    InvalidInstrument,
    InvalidPrice,
    InvalidQuantity,
    QuantityLimit,
    NotionalLimit,
    PositionLimit,
    OpenOrderLimit,
    PriceCollar,
    DuplicateOrderId,
    UnknownOrder,
    NotOrderOwner,
    KillSwitch,
    UnknownAccount,
    ArithmeticOverflow,
    OrderCapacity,
    PriceLevelCapacity,
    PriceLevelOrderCapacity,
    ReportCapacity,
    InsufficientLiquidity,
    PostOnlyWouldTrade,
    /// A replace tried to move an order to a price that would cross.
    ReplaceWouldCross,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct ExecutionReport {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub instrument_id: InstrumentId,
    pub price: PriceTicks,
    pub quantity: Quantity,
    pub sequence: SequenceNumber,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MatchSummary {
    pub state: OrderState,
    pub filled_quantity: Quantity,
    pub resting_quantity: Quantity,
    /// IOC remainder that crossed nothing and was discarded.
    pub discarded_quantity: Quantity,
    pub report_count: usize,
}

pub struct ReportBuffer<const N: usize> {
    entries: [ExecutionReport; N],
    len: usize,
}

impl<const N: usize> ReportBuffer<N> {
    const EMPTY_REPORT: ExecutionReport = ExecutionReport {
        maker_order_id: OrderId(0),
        taker_order_id: OrderId(0),
        instrument_id: InstrumentId(0),
        price: PriceTicks(0),
        quantity: Quantity(0),
        sequence: SequenceNumber(0),
    };

    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: [Self::EMPTY_REPORT; N],
            len: 0,
        }
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// # Errors
    ///
    /// Returns [`RejectReason::ReportCapacity`] when the fixed buffer is full.
    pub fn push(&mut self, report: ExecutionReport) -> Result<(), RejectReason> {
        let Some(slot) = self.entries.get_mut(self.len) else {
            return Err(RejectReason::ReportCapacity);
        };
        *slot = report;
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

    #[must_use]
    pub const fn remaining_capacity(&self) -> usize {
        N - self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = &ExecutionReport> {
        self.entries[..self.len].iter()
    }
}

impl<const N: usize> core::fmt::Debug for ReportBuffer<N> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ReportBuffer")
            .field("entries", &&self.entries[..self.len])
            .field("len", &self.len)
            .finish()
    }
}

impl<const N: usize> PartialEq for ReportBuffer<N> {
    fn eq(&self, other: &Self) -> bool {
        self.entries[..self.len] == other.entries[..other.len]
    }
}

impl<const N: usize> Eq for ReportBuffer<N> {}

impl<const N: usize> Default for ReportBuffer<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_buffer_rejects_overflow() {
        let report = ExecutionReport {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            instrument_id: InstrumentId(3),
            price: PriceTicks(4),
            quantity: Quantity(5),
            sequence: SequenceNumber(6),
        };
        let mut reports = ReportBuffer::<1>::new();
        assert_eq!(reports.push(report), Ok(()));
        assert_eq!(reports.push(report), Err(RejectReason::ReportCapacity));
    }

    #[test]
    fn cleared_report_buffers_ignore_old_entries() {
        let report = ExecutionReport {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            instrument_id: InstrumentId(3),
            price: PriceTicks(4),
            quantity: Quantity(5),
            sequence: SequenceNumber(6),
        };
        let mut reports = ReportBuffer::<2>::new();
        reports.push(report).expect("buffer has capacity");
        reports.clear();
        assert_eq!(reports, ReportBuffer::new());
    }

    #[test]
    fn direct_report_storage_is_smaller_than_option_slots() {
        assert!(
            core::mem::size_of::<ReportBuffer<64>>()
                < core::mem::size_of::<[Option<ExecutionReport>; 64]>()
                    + core::mem::size_of::<usize>()
        );
    }
}
