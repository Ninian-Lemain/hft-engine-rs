use crate::record::{BenchRecord, Extra};
use crate::{allocation_gate, assert_allocation_gate, push_latency_record};
use hft_book::{OrderBook, TopLevel};
use hft_types::{
    AccountId, CancelOrder, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, ReportBuffer,
    SequenceNumber, Side, TimeInForce,
};
use std::hint::black_box;
use std::time::Instant;

const WARMUP: usize = 64;
const INSTRUMENT: InstrumentId = InstrumentId(1);

pub fn liquidity_benchmarks(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    top_level_cell::<8>(samples, out);
    top_level_cell::<64>(samples, out);
    top_level_cell::<512>(samples, out);
    update_top_cell(samples, out);
}

fn order(id: u64, side: Side, quantity: u64) -> NewOrder {
    NewOrder {
        order_id: OrderId(id),
        account_id: AccountId(1),
        instrument_id: INSTRUMENT,
        price: PriceTicks(if side == Side::Buy { 99 } else { 100 }),
        quantity: Quantity(quantity),
        sequence: SequenceNumber(id),
        side,
        time_in_force: TimeInForce::Gtc,
    }
}

fn fixture<const CAPACITY: usize>(depth: usize) -> OrderBook<1, CAPACITY> {
    let mut book = OrderBook::new(INSTRUMENT);
    let mut reports = ReportBuffer::<1>::new();
    for offset in 0..depth {
        let offset = u64::try_from(offset).expect("fixture offset fits u64");
        for (id, side) in [(offset * 2 + 1, Side::Buy), (offset * 2 + 2, Side::Sell)] {
            let summary = book
                .submit(order(id, side, offset % 7 + 1), &mut reports)
                .expect("fixture order rests");
            assert_eq!(summary.resting_quantity, Quantity(offset % 7 + 1));
            assert!(reports.is_empty());
        }
    }
    book
}

fn inspect<const CAPACITY: usize>(book: &OrderBook<1, CAPACITY>) -> (TopLevel, TopLevel) {
    let bid = black_box(book)
        .top_level(black_box(Side::Buy))
        .expect("fixture bid exists");
    let ask = black_box(book)
        .top_level(black_box(Side::Sell))
        .expect("fixture ask exists");
    black_box((bid, ask))
}

fn checksum(top: (TopLevel, TopLevel)) -> u64 {
    let bid_quantity = u64::try_from(top.0.aggregate_quantity).expect("fixture quantity fits u64");
    let ask_quantity = u64::try_from(top.1.aggregate_quantity).expect("fixture quantity fits u64");
    bid_quantity.rotate_left(7)
        ^ ask_quantity
        ^ u64::try_from(top.0.order_count)
            .expect("fixture count fits u64")
            .rotate_left(17)
        ^ u64::try_from(top.1.order_count)
            .expect("fixture count fits u64")
            .rotate_left(29)
}

fn top_level_cell<const DEPTH: usize>(samples: usize, out: &mut Vec<BenchRecord>) {
    let book = fixture::<DEPTH>(DEPTH);
    let expected_quantity = (0..DEPTH).map(|offset| (offset % 7 + 1) as u128).sum();
    let expected = (
        TopLevel {
            price: PriceTicks(99),
            aggregate_quantity: expected_quantity,
            order_count: DEPTH,
        },
        TopLevel {
            price: PriceTicks(100),
            aggregate_quantity: expected_quantity,
            order_count: DEPTH,
        },
    );
    assert_eq!(inspect(&book), expected);
    for _ in 0..WARMUP {
        black_box(inspect(&book));
    }
    let mut latencies = vec![0; samples];
    let mut digest = 0_u64;
    let gate = allocation_gate();
    for sample in &mut latencies {
        let started = Instant::now();
        let top = inspect(&book);
        *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        assert_eq!(top, expected);
        digest = digest.wrapping_add(checksum(top));
    }
    assert_allocation_gate(gate, "top level inspection");
    push_latency_record(
        out,
        BenchRecord {
            checksum: digest,
            ..BenchRecord::new(
                "component",
                "book",
                "top_level_pair",
                &[
                    (
                        "depth",
                        Extra::U64(u64::try_from(DEPTH).expect("depth fits u64")),
                    ),
                    (
                        "book_bytes",
                        Extra::U64(
                            u64::try_from(size_of::<OrderBook<1, DEPTH>>())
                                .expect("book size fits u64"),
                        ),
                    ),
                ],
            )
        },
        &mut latencies,
    );
}

fn update_top_cell(samples: usize, out: &mut Vec<BenchRecord>) {
    const DEPTH: usize = 64;
    let mut book = fixture::<65>(DEPTH);
    let expected = inspect(&book);
    let mut reports = ReportBuffer::<1>::new();
    let mut latencies = vec![0; samples];
    let mut digest = 0_u64;
    let gate = allocation_gate();
    for step in 0..WARMUP + samples {
        let id = u64::try_from(step).expect("sample index fits u64") + 1_000;
        let command = order(id, Side::Sell, 3);
        let cancel = CancelOrder {
            order_id: command.order_id,
            account_id: command.account_id,
            instrument_id: INSTRUMENT,
            sequence: command.sequence,
        };
        let started = Instant::now();
        let submitted = black_box(&mut book).submit(black_box(command), &mut reports);
        let after_submit = inspect(&book);
        let cancelled = black_box(&mut book).cancel(black_box(cancel));
        let after_cancel = inspect(&book);
        black_box((&submitted, &cancelled));
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(
            submitted.expect("cycle order rests").resting_quantity,
            Quantity(3)
        );
        assert_eq!(
            cancelled.expect("cycle order cancels").quantity,
            Quantity(3)
        );
        assert!(reports.is_empty());
        assert_eq!(after_submit.0, expected.0);
        assert_eq!(after_submit.1.price, expected.1.price);
        assert_eq!(after_submit.1.order_count, DEPTH + 1);
        assert_eq!(
            after_submit.1.aggregate_quantity,
            expected.1.aggregate_quantity + 3
        );
        assert_eq!(after_cancel, expected);
        if step >= WARMUP {
            latencies[step - WARMUP] = u64::try_from(elapsed).unwrap_or(u64::MAX);
            digest = digest.wrapping_add(checksum(after_submit) ^ checksum(after_cancel));
        }
    }
    assert_allocation_gate(gate, "submit inspect cancel inspect");
    push_latency_record(
        out,
        BenchRecord {
            checksum: digest,
            ..BenchRecord::new(
                "component",
                "book",
                "submit_top_cancel_top",
                &[
                    ("depth", Extra::U64(64)),
                    ("orders_per_level", Extra::U64(65)),
                    (
                        "book_bytes",
                        Extra::U64(
                            u64::try_from(size_of::<OrderBook<1, 65>>())
                                .expect("book size fits u64"),
                        ),
                    ),
                ],
            )
        },
        &mut latencies,
    );
}
