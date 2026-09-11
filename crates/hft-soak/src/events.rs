use hft_events::{Event, EventBatch, EventId};
use hft_types::{InstrumentId, OrderState, RejectReason, SequenceNumber};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventSummary {
    pub events: u64,
    pub terminals: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub cancelled: u64,
    pub replaced: u64,
    pub trades: u64,
    pub top_of_book: u64,
    pub fingerprint: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventValidationError {
    EmptyBatch,
    WrongInstrument,
    WrongSequence,
    WrongOrdinal,
    TerminalNotFirst,
    TerminalCount,
    DuplicateTopOfBook,
    TopOfBookNotLast,
    UnexpectedTrade,
    UnexpectedTopOfBook,
}

impl fmt::Display for EventValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyBatch => "event batch is empty",
            Self::WrongInstrument => "event instrument does not match its shard",
            Self::WrongSequence => "event sequence does not match its command",
            Self::WrongOrdinal => "event ordinals are not contiguous",
            Self::TerminalNotFirst => "terminal event is not first",
            Self::TerminalCount => "event batch does not contain one terminal event",
            Self::DuplicateTopOfBook => "event batch contains multiple top of book events",
            Self::TopOfBookNotLast => "top of book event is not last",
            Self::UnexpectedTrade => "trade follows a nonaccepted command",
            Self::UnexpectedTopOfBook => "top of book follows a rejected command",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for EventValidationError {}

/// Checks event identity and ordering within one command batch.
///
/// # Errors
///
/// Returns the first identity, order, terminal, or top of book violation.
pub fn validate_batch<const N: usize>(
    batch: &EventBatch<N>,
    instrument: InstrumentId,
    sequence: SequenceNumber,
) -> Result<EventSummary, EventValidationError> {
    if batch.is_empty() {
        return Err(EventValidationError::EmptyBatch);
    }

    let mut summary = EventSummary::default();
    let mut terminal_count = 0_u64;
    let mut top_count = 0_u64;
    let accepted = matches!(batch.iter().next(), Some(Event::Accepted(_)));
    let rejected = matches!(batch.iter().next(), Some(Event::Rejected(_)));
    for (index, event) in batch.iter().enumerate() {
        let id = event_id(event);
        if event_instrument(event) != instrument {
            return Err(EventValidationError::WrongInstrument);
        }
        if id.command_sequence != sequence {
            return Err(EventValidationError::WrongSequence);
        }
        if usize::from(id.ordinal) != index {
            return Err(EventValidationError::WrongOrdinal);
        }

        if is_terminal(event) {
            terminal_count += 1;
            if index != 0 {
                return Err(EventValidationError::TerminalNotFirst);
            }
        }
        match event {
            Event::Trade(_) => {
                if !accepted {
                    return Err(EventValidationError::UnexpectedTrade);
                }
                summary.trades += 1;
            }
            Event::TopOfBook(_) => {
                if rejected {
                    return Err(EventValidationError::UnexpectedTopOfBook);
                }
                top_count += 1;
                summary.top_of_book += 1;
                if top_count > 1 {
                    return Err(EventValidationError::DuplicateTopOfBook);
                }
                if index + 1 != batch.len() {
                    return Err(EventValidationError::TopOfBookNotLast);
                }
            }
            Event::Accepted(_) => summary.accepted += 1,
            Event::Rejected(_) => summary.rejected += 1,
            Event::Cancelled(_) => summary.cancelled += 1,
            Event::Replaced(_) => summary.replaced += 1,
        }
        summary.events += 1;
        summary.fingerprint = hash_event(summary.fingerprint, event);
    }

    if terminal_count != 1 {
        return Err(EventValidationError::TerminalCount);
    }
    summary.terminals = terminal_count;
    Ok(summary)
}

fn event_id(event: &Event) -> EventId {
    match event {
        Event::Accepted(value) => value.id,
        Event::Rejected(value) => value.id,
        Event::Trade(value) => value.id,
        Event::Cancelled(value) => value.id,
        Event::Replaced(value) => value.id,
        Event::TopOfBook(value) => value.id,
    }
}

fn event_instrument(event: &Event) -> InstrumentId {
    match event {
        Event::Accepted(value) => value.instrument_id,
        Event::Rejected(value) => value.instrument_id,
        Event::Trade(value) => value.instrument_id,
        Event::Cancelled(value) => value.instrument_id,
        Event::Replaced(value) => value.instrument_id,
        Event::TopOfBook(value) => value.instrument_id,
    }
}

const fn is_terminal(event: &Event) -> bool {
    matches!(
        event,
        Event::Accepted(_) | Event::Rejected(_) | Event::Cancelled(_) | Event::Replaced(_)
    )
}

fn hash_event(mut hash: u64, event: &Event) -> u64 {
    let id = event_id(event);
    hash = mix(hash, id.command_sequence.0);
    hash = mix(hash, u64::from(id.ordinal));
    hash = mix(hash, u64::from(event_instrument(event).0));
    match event {
        Event::Accepted(value) => {
            hash = mix(hash, 1);
            hash = mix(hash, value.order_id.0);
            hash = mix(hash, u64::from(value.account_id.0));
            hash = mix(hash, order_state(value.state));
            hash = mix(hash, value.filled_quantity.0);
            hash = mix(hash, value.resting_quantity.0);
            mix(hash, value.discarded_quantity.0)
        }
        Event::Rejected(value) => {
            hash = mix(hash, 2);
            hash = mix(hash, value.order_id.0);
            hash = mix(hash, u64::from(value.account_id.0));
            hash = mix(hash, value.command as u64);
            mix(hash, reject_reason(value.reason))
        }
        Event::Trade(value) => {
            hash = mix(hash, 3);
            hash = mix(hash, value.maker_order_id.0);
            hash = mix(hash, value.taker_order_id.0);
            hash = mix(hash, u64::from_be_bytes(value.price.0.to_be_bytes()));
            mix(hash, value.quantity.0)
        }
        Event::Cancelled(value) => {
            hash = mix(hash, 4);
            hash = mix(hash, value.order_id.0);
            hash = mix(hash, u64::from(value.account_id.0));
            mix(hash, value.quantity.0)
        }
        Event::Replaced(value) => {
            hash = mix(hash, 5);
            hash = mix(hash, value.order_id.0);
            hash = mix(hash, u64::from(value.account_id.0));
            hash = mix(hash, value.old_quantity.0);
            hash = mix(hash, value.new_quantity.0);
            hash = mix(hash, u64::from_be_bytes(value.price.0.to_be_bytes()));
            mix(hash, u64::from(value.priority_lost))
        }
        Event::TopOfBook(value) => {
            hash = mix(hash, 6);
            hash = hash_top(hash, value.bid);
            hash_top(hash, value.ask)
        }
    }
}

fn hash_top(mut hash: u64, top: Option<hft_book::TopLevel>) -> u64 {
    let Some(top) = top else {
        return mix(hash, 0);
    };
    hash = mix(hash, 1);
    hash = mix(hash, u64::from_be_bytes(top.price.0.to_be_bytes()));
    let quantity = top.aggregate_quantity.to_be_bytes();
    hash = mix(
        hash,
        u64::from_be_bytes([
            quantity[0],
            quantity[1],
            quantity[2],
            quantity[3],
            quantity[4],
            quantity[5],
            quantity[6],
            quantity[7],
        ]),
    );
    hash = mix(
        hash,
        u64::from_be_bytes([
            quantity[8],
            quantity[9],
            quantity[10],
            quantity[11],
            quantity[12],
            quantity[13],
            quantity[14],
            quantity[15],
        ]),
    );
    mix(hash, u64::try_from(top.order_count).unwrap_or(u64::MAX))
}

const fn order_state(state: OrderState) -> u64 {
    match state {
        OrderState::Accepted => 1,
        OrderState::PartiallyFilled => 2,
        OrderState::Filled => 3,
        OrderState::Rejected(reason) => 0x100 | reject_reason(reason),
    }
}

const fn reject_reason(reason: RejectReason) -> u64 {
    match reason {
        RejectReason::InvalidInstrument => 1,
        RejectReason::InvalidPrice => 2,
        RejectReason::InvalidQuantity => 3,
        RejectReason::QuantityLimit => 4,
        RejectReason::NotionalLimit => 5,
        RejectReason::PositionLimit => 6,
        RejectReason::OpenOrderLimit => 7,
        RejectReason::PriceCollar => 8,
        RejectReason::DuplicateOrderId => 9,
        RejectReason::UnknownOrder => 10,
        RejectReason::NotOrderOwner => 11,
        RejectReason::KillSwitch => 12,
        RejectReason::UnknownAccount => 13,
        RejectReason::ArithmeticOverflow => 14,
        RejectReason::OrderCapacity => 15,
        RejectReason::PriceLevelCapacity => 16,
        RejectReason::PriceLevelOrderCapacity => 17,
        RejectReason::ReportCapacity => 18,
        RejectReason::InsufficientLiquidity => 19,
        RejectReason::PostOnlyWouldTrade => 20,
        RejectReason::ReplaceWouldCross => 21,
    }
}

const fn mix(hash: u64, value: u64) -> u64 {
    hash.rotate_left(9)
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ value.wrapping_mul(0x94d0_49bb_1331_11eb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_events::{Accepted, TopOfBook};
    use hft_types::{AccountId, OrderId, Quantity};

    #[test]
    fn validates_terminal_and_top_order() {
        let instrument = InstrumentId(11);
        let sequence = SequenceNumber(7);
        let mut batch = EventBatch::<2>::new();
        batch
            .try_push(Event::Accepted(Accepted {
                id: EventId {
                    command_sequence: sequence,
                    ordinal: 0,
                },
                order_id: OrderId(1),
                account_id: AccountId(1),
                instrument_id: instrument,
                state: OrderState::Accepted,
                filled_quantity: Quantity(0),
                resting_quantity: Quantity(2),
                discarded_quantity: Quantity(0),
            }))
            .expect("terminal");
        batch
            .try_push(Event::TopOfBook(TopOfBook {
                id: EventId {
                    command_sequence: sequence,
                    ordinal: 1,
                },
                instrument_id: instrument,
                bid: None,
                ask: None,
            }))
            .expect("top");

        let first = validate_batch(&batch, instrument, sequence).expect("valid batch");
        let second = validate_batch(&batch, instrument, sequence).expect("repeat validation");
        assert_eq!(first, second);
        assert_eq!(first.events, 2);
        assert_eq!(first.terminals, 1);
        assert_eq!(first.accepted, 1);
        assert_eq!(first.top_of_book, 1);
    }

    #[test]
    fn nonaccepted_commands_cannot_publish_trades() {
        use hft_events::{Cancelled, CommandKind, Rejected, Replaced, Trade};
        use hft_types::PriceTicks;

        let instrument_id = InstrumentId(11);
        let sequence = SequenceNumber(7);
        let id = EventId {
            command_sequence: sequence,
            ordinal: 0,
        };
        let terminals = [
            Event::Rejected(Rejected {
                id,
                command: CommandKind::NewOrder,
                order_id: OrderId(2),
                account_id: AccountId(1),
                instrument_id,
                reason: RejectReason::QuantityLimit,
            }),
            Event::Cancelled(Cancelled {
                id,
                order_id: OrderId(2),
                account_id: AccountId(1),
                instrument_id,
                quantity: Quantity(1),
            }),
            Event::Replaced(Replaced {
                id,
                order_id: OrderId(2),
                account_id: AccountId(1),
                instrument_id,
                old_quantity: Quantity(1),
                new_quantity: Quantity(2),
                price: PriceTicks(100),
                priority_lost: true,
            }),
        ];
        for terminal in terminals {
            let mut batch = EventBatch::<2>::new();
            batch.try_push(terminal).expect("terminal");
            batch
                .try_push(Event::Trade(Trade {
                    id: EventId { ordinal: 1, ..id },
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(2),
                    instrument_id,
                    price: PriceTicks(100),
                    quantity: Quantity(1),
                }))
                .expect("trade");
            assert_eq!(
                validate_batch(&batch, instrument_id, sequence),
                Err(EventValidationError::UnexpectedTrade),
            );
        }

        let mut batch = EventBatch::<2>::new();
        batch.try_push(terminals[0]).expect("rejection");
        batch
            .try_push(Event::TopOfBook(TopOfBook {
                id: EventId { ordinal: 1, ..id },
                instrument_id,
                bid: None,
                ask: None,
            }))
            .expect("top");
        assert_eq!(
            validate_batch(&batch, instrument_id, sequence),
            Err(EventValidationError::UnexpectedTopOfBook),
        );
    }
}
