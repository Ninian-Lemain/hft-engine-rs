//! Time-in-force workloads: IOC and FOK submission paths timed against
//! equivalent GTC paths on one mid-price book. Every scenario rests its
//! fixture makers up front, warms up untimed, gates allocations around the
//! sampling window, and restores the steady state with untimed teardown work
//! between samples.

use crate::record::{BenchRecord, Extra};
use crate::{ALLOCATIONS, DEALLOCATIONS, analyze};
use hft_book::OrderBook;
use hft_types::{
    AccountId, CancelOrder, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, RejectReason,
    ReplaceOrder, ReportBuffer, SequenceNumber, Side, TimeInForce,
};
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Book shape shared by every time-in-force scenario.
type BenchBook = OrderBook<128, 8>;

/// Report capacity shared by every scenario.
type Reports = ReportBuffer<16>;

const INSTRUMENT: InstrumentId = InstrumentId(1);
const MAKER_ACCOUNT: AccountId = AccountId(1);
const TAKER_ACCOUNT: AccountId = AccountId(2);

/// Untimed iterations per scenario before sampling starts.
const WARMUP_SAMPLES: usize = 64;

/// Checksum contribution for scenarios whose submit rejects.
const REJECT_CHECKSUM: u64 = 0x0F0F_F00D;

/// Measures IOC and FOK submit latency against equivalent GTC paths, one
/// record per scenario. Only the submit call is timed; fixture repair and
/// replenishment stay outside the clock, so each sample observes the intended
/// steady-state occupancy.
///
/// # Panics
///
/// Panics when a fixture assertion fails, a fill quantity deviates from the
/// expected amount, or an allocation gate trips between the pre-sampling
/// snapshot and the end of a sampling window.
pub fn tif_benchmark(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    let mut next_taker_id = 1_000_000_u64;
    let mut replenish_id = 5_000_000_u64;
    run_ioc_empty(samples, &mut next_taker_id, &mut replenish_id, out);
    run_ioc_partial(samples, &mut next_taker_id, &mut replenish_id, out);
    run_ioc_full(samples, &mut next_taker_id, &mut replenish_id, out);
    run_fok_reject(samples, &mut next_taker_id, &mut replenish_id, out);
    run_fok_single(samples, &mut next_taker_id, &mut replenish_id, out);
    run_fok_multi(samples, &mut next_taker_id, &mut replenish_id, out);
    run_gtc_full(samples, &mut next_taker_id, &mut replenish_id, out);
    run_gtc_partial_rests(samples, &mut next_taker_id, &mut replenish_id, out);
    run_po_cell(
        samples,
        "post_only_cross_shallow",
        false,
        &mut next_taker_id,
        out,
    );
    run_po_cell(
        samples,
        "post_only_cross_deep",
        true,
        &mut next_taker_id,
        out,
    );
    run_po_cell(
        samples,
        "post_only_noncross_shallow",
        false,
        &mut next_taker_id,
        out,
    );
    run_po_cell(
        samples,
        "post_only_noncross_deep",
        true,
        &mut next_taker_id,
        out,
    );
}

/// Builds a taker or maker order with the sequence number tracking the id.
fn order(
    id: u64,
    account: AccountId,
    price: i64,
    qty: u64,
    side: Side,
    tif: TimeInForce,
) -> NewOrder {
    NewOrder {
        time_in_force: tif,
        order_id: OrderId(id),
        account_id: account,
        instrument_id: INSTRUMENT,
        price: PriceTicks(price),
        quantity: Quantity(qty),
        sequence: SequenceNumber(id),
        side,
    }
}

/// Draws the next monotonic id from a counter starting at its initial value.
fn take_id(counter: &mut u64) -> u64 {
    let id = *counter;
    *counter += 1;
    id
}

/// Rests one GTC maker on the ask side and resets the report buffer.
fn rest_fixture(
    book: &mut BenchBook,
    reports: &mut Reports,
    replenish_id: &mut u64,
    price: i64,
    qty: u64,
) {
    let id = take_id(replenish_id);
    book.submit(
        order(id, MAKER_ACCOUNT, price, qty, Side::Sell, TimeInForce::Gtc),
        reports,
    )
    .expect("fixture maker rests");
    reports.clear();
}

/// Loads both allocation counters immediately before a sampling window.
fn snapshot_gate() -> (u64, u64) {
    (
        ALLOCATIONS.load(Ordering::SeqCst),
        DEALLOCATIONS.load(Ordering::SeqCst),
    )
}

/// Asserts the allocation counters are unchanged across one sampling window.
fn assert_gate(before: (u64, u64), after: (u64, u64), scenario: &'static str) {
    assert_eq!(after.0, before.0, "{scenario}: allocations");
    assert_eq!(after.1, before.1, "{scenario}: deallocations");
}

/// Computes latency statistics over the sample slice, completes the pending
/// record fields, and appends the record.
fn finish(
    out: &mut Vec<BenchRecord>,
    mut record: BenchRecord,
    checksum: u64,
    latencies: &mut [u64],
) {
    let stats = analyze(latencies);
    let mean_ns = u64::try_from(stats.mean).unwrap_or(u64::MAX);
    record.samples = latencies.len();
    record.mean_ns = mean_ns;
    record.p50_ns = stats.p50;
    record.p90_ns = stats.p90;
    record.p99_ns = stats.p99;
    record.p99_9_ns = stats.p99_9;
    record.max_ns = stats.max;
    record.ops_per_second = 1_000_000_000_u64.checked_div(mean_ns).unwrap_or(0);
    record.checksum = checksum;
    out.push(record);
}

/// Stages consumed prices on the stack, clears the report buffer, then
/// re-rests one unit at each consumed price with a fresh maker id.
fn replenish_fan(book: &mut BenchBook, reports: &mut Reports, replenish_id: &mut u64) {
    let mut staged_prices = [0_i64; 16];
    let staged = reports.len().min(staged_prices.len());
    for (slot, report) in staged_prices.iter_mut().zip(reports.iter()) {
        *slot = report.price.0;
    }
    reports.clear();
    for &price in &staged_prices[..staged] {
        rest_fixture(book, reports, replenish_id, price, 1);
    }
}

/// Cancels the rested GTC remainder, then replenishes the consumed maker.
fn teardown_gtc_partial(
    book: &mut BenchBook,
    reports: &mut Reports,
    rested_bid: u64,
    replenish_id: &mut u64,
) {
    book.cancel(CancelOrder {
        order_id: OrderId(rested_bid),
        account_id: TAKER_ACCOUNT,
        instrument_id: INSTRUMENT,
        sequence: SequenceNumber(rested_bid),
    })
    .expect("rested GTC remainder cancels");
    rest_fixture(book, reports, replenish_id, 100, 3);
}

/// Times one IOC probe priced below the market: nothing fills, nothing rests.
fn step_ioc_empty(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 99, 10, Side::Buy, TimeInForce::Ioc),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("below-market IOC accepts");
    assert_eq!(
        summary.filled_quantity.0, 0,
        "below-market IOC fills nothing"
    );
    assert_eq!(summary.report_count, 0, "below-market IOC emits no reports");
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one IOC crossing a shallow ask for a partial fill.
fn step_ioc_partial(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 5, Side::Buy, TimeInForce::Ioc),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("IOC partial submits");
    assert_eq!(summary.filled_quantity.0, 3, "IOC partial fills the ask");
    assert_eq!(summary.resting_quantity.0, 0, "IOC remainder never rests");
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one IOC filling four units from a deep ask.
fn step_ioc_full(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 4, Side::Buy, TimeInForce::Ioc),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("IOC full submits");
    assert_eq!(
        summary.filled_quantity.0, 4,
        "IOC fills the requested units"
    );
    assert_eq!(summary.resting_quantity.0, 0, "IOC remainder never rests");
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one FOK rejection for insufficient liquidity; the book is untouched.
fn step_fok_reject(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 5, Side::Buy, TimeInForce::Fok),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let rejected = submitted.expect_err("undersized FOK rejects");
    assert!(
        matches!(rejected, RejectReason::InsufficientLiquidity),
        "FOK rejects for missing liquidity"
    );
    assert_eq!(
        book.order_count(),
        1,
        "rejected FOK leaves the maker resting"
    );
    *checksum ^= REJECT_CHECKSUM;
    elapsed
}

/// Times one FOK filling a single unit from a deep ask.
fn step_fok_single(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 1, Side::Buy, TimeInForce::Fok),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("FOK single submits");
    assert_eq!(summary.filled_quantity.0, 1, "FOK single fills one unit");
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one FOK sweeping eight single-unit asks across eight price levels.
fn step_fok_multi(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 1_000, 8, Side::Buy, TimeInForce::Fok),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("FOK multi sweeps the fan");
    assert_eq!(summary.filled_quantity.0, 8, "FOK multi fills eight units");
    assert_eq!(summary.report_count, 8, "one report per consumed level");
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one GTC filling four units from a deep ask, matching `ioc_full`.
fn step_gtc_full(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 4, Side::Buy, TimeInForce::Gtc),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("GTC full submits");
    assert_eq!(summary.filled_quantity.0, 4, "GTC full fills the ask units");
    assert_eq!(
        summary.resting_quantity.0, 0,
        "fully filled GTC never rests"
    );
    *checksum ^= summary.filled_quantity.0;
    elapsed
}

/// Times one GTC partially filling and resting the remainder as a bid.
/// Returns the elapsed nanoseconds and the rested bid id for the teardown.
fn step_gtc_partial_rests(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> (u128, u64) {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 5, Side::Buy, TimeInForce::Gtc),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let summary = submitted.expect("GTC partial submits");
    assert_eq!(summary.filled_quantity.0, 3, "GTC partial fills the ask");
    assert_eq!(
        summary.resting_quantity.0, 2,
        "GTC remainder rests as a bid"
    );
    *checksum ^= summary.filled_quantity.0;
    (elapsed, id)
}

/// Below-market IOC probes against one deep ask: zero-fill fast path.
fn run_ioc_empty(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 10_000_000);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_ioc_empty(&mut book, &mut reports, next_taker_id, &mut checksum);
        reports.clear();
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_ioc_empty(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        reports.clear();
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif ioc_empty");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "ioc_empty",
            &[("tif", Extra::Text("ioc"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// IOC partial fills against a three-unit ask restored between samples.
fn run_ioc_partial(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 3);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_ioc_partial(&mut book, &mut reports, next_taker_id, &mut checksum);
        rest_fixture(&mut book, &mut reports, replenish_id, 100, 3);
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_ioc_partial(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        rest_fixture(&mut book, &mut reports, replenish_id, 100, 3);
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif ioc_partial");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "ioc_partial",
            &[("tif", Extra::Text("ioc"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// IOC full fills from a deep ask that never depletes.
fn run_ioc_full(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 10_000_000);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_ioc_full(&mut book, &mut reports, next_taker_id, &mut checksum);
        reports.clear();
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_ioc_full(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        reports.clear();
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif ioc_full");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "ioc_full",
            &[("tif", Extra::Text("ioc"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// FOK rejection against a shallow ask; the fixture survives every attempt.
fn run_fok_reject(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 3);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_fok_reject(&mut book, &mut reports, next_taker_id, &mut checksum);
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_fok_reject(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif fok_reject");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "fok_reject",
            &[("tif", Extra::Text("fok"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// FOK single-unit fills from a deep ask that never depletes.
fn run_fok_single(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 10_000_000);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_fok_single(&mut book, &mut reports, next_taker_id, &mut checksum);
        reports.clear();
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_fok_single(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        reports.clear();
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif fok_single");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "fok_single",
            &[("tif", Extra::Text("fok"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// FOK sweeps of an eight-level fan, re-rested from staged prices each sample.
fn run_fok_multi(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    for offset in 0..8_usize {
        let price = 100 + i64::try_from(offset).unwrap_or(i64::MAX);
        rest_fixture(&mut book, &mut reports, replenish_id, price, 1);
    }
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_fok_multi(&mut book, &mut reports, next_taker_id, &mut checksum);
        replenish_fan(&mut book, &mut reports, replenish_id);
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_fok_multi(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        replenish_fan(&mut book, &mut reports, replenish_id);
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif fok_multi");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "fok_multi",
            &[("tif", Extra::Text("fok"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// GTC full fills, the direct comparison cell for `ioc_full`.
fn run_gtc_full(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 10_000_000);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        step_gtc_full(&mut book, &mut reports, next_taker_id, &mut checksum);
        reports.clear();
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = step_gtc_full(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        reports.clear();
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif gtc_full");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "gtc_full",
            &[("tif", Extra::Text("gtc"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// GTC partial fills that rest two units; the bid is cancelled untimed and
/// the maker replenished before the next sample.
fn run_gtc_partial_rests(
    samples: usize,
    next_taker_id: &mut u64,
    replenish_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    let mut book = std::boxed::Box::new(BenchBook::new(INSTRUMENT));
    let mut reports = Reports::new();
    rest_fixture(&mut book, &mut reports, replenish_id, 100, 3);
    let mut checksum = 0_u64;
    for _ in 0..WARMUP_SAMPLES {
        let (_, rested_bid) =
            step_gtc_partial_rests(&mut book, &mut reports, next_taker_id, &mut checksum);
        teardown_gtc_partial(&mut book, &mut reports, rested_bid, replenish_id);
    }
    let mut latencies = vec![0_u64; samples];
    let before = snapshot_gate();
    for sample in &mut latencies {
        let (elapsed, rested_bid) =
            step_gtc_partial_rests(&mut book, &mut reports, next_taker_id, &mut checksum);
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
        teardown_gtc_partial(&mut book, &mut reports, rested_bid, replenish_id);
    }
    let after = snapshot_gate();
    assert_gate(before, after, "tif gtc_partial_rests");
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            "gtc_partial_rests",
            &[("tif", Extra::Text("gtc"))],
        ),
        checksum,
        &mut latencies,
    );
}

/// Reject checksum so crossing-reject cells carry a non-trivial value.
const PO_REJECT_CHECKSUM: u64 = 0x0070_6f72_656a;

fn po_cross_step(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let submitted = book.submit(
        order(id, TAKER_ACCOUNT, 100, 5, Side::Buy, TimeInForce::PostOnly),
        reports,
    );
    let elapsed = started.elapsed().as_nanos();
    let rejected = submitted.expect_err("crossing post-only rejects");
    assert!(
        matches!(rejected, RejectReason::PostOnlyWouldTrade),
        "post-only reject reason"
    );
    *checksum ^= PO_REJECT_CHECKSUM;
    elapsed
}

fn po_noncross_step(
    book: &mut BenchBook,
    reports: &mut Reports,
    next_taker_id: &mut u64,
    checksum: &mut u64,
) -> u128 {
    let id = take_id(next_taker_id);
    let started = Instant::now();
    let summary = book
        .submit(
            order(id, TAKER_ACCOUNT, 99, 1, Side::Buy, TimeInForce::PostOnly),
            reports,
        )
        .expect("non-crossing post only rests");
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(summary.filled_quantity.0, 0);
    assert_eq!(summary.resting_quantity.0, 1);
    *checksum ^= summary.resting_quantity.0;
    book.cancel(CancelOrder {
        order_id: OrderId(id),
        account_id: TAKER_ACCOUNT,
        instrument_id: INSTRUMENT,
        sequence: SequenceNumber(id),
    })
    .expect("teardown cancel");
    reports.clear();
    elapsed
}

fn run_po_cell(
    samples: usize,
    scenario: &'static str,
    deep: bool,
    next_taker_id: &mut u64,
    out: &mut Vec<BenchRecord>,
) {
    const WARMUP: usize = 32;
    if samples == 0 {
        return;
    }
    let mut replenish_id = 9_000_000_u64;
    let mut book = BenchBook::new(INSTRUMENT);
    let mut reports = ReportBuffer::<16>::new();
    let maker_count = if deep { 64 } else { 1 };
    for level_index in 0..maker_count {
        rest_fixture(
            &mut book,
            &mut reports,
            &mut replenish_id,
            100 + i64::from(level_index),
            10_000_000,
        );
    }
    let mut latencies = vec![0_u64; samples];
    let mut checksum = 0_u64;
    for _ in 0..WARMUP {
        if deep {
            po_cross_step(&mut book, &mut reports, next_taker_id, &mut checksum);
        } else {
            po_noncross_step(&mut book, &mut reports, next_taker_id, &mut checksum);
        }
    }
    let before = snapshot_gate();
    for sample in &mut latencies {
        let elapsed = if deep {
            po_cross_step(&mut book, &mut reports, next_taker_id, &mut checksum)
        } else {
            po_noncross_step(&mut book, &mut reports, next_taker_id, &mut checksum)
        };
        *sample = u64::try_from(elapsed).unwrap_or(u64::MAX);
    }
    let after = (
        ALLOCATIONS.load(Ordering::SeqCst),
        DEALLOCATIONS.load(Ordering::SeqCst),
    );
    assert_gate(before, after, scenario);
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            scenario,
            &[
                ("tif", Extra::Text("post_only")),
                (
                    "levels",
                    Extra::U64(u64::try_from(maker_count).unwrap_or(u64::MAX)),
                ),
            ],
        ),
        checksum,
        &mut latencies,
    );
}

/// Replace benchmarks: book mutation cost versus risk adjustment cost,
/// measured separately on pre-seeded fixtures.
use hft_risk::RiskEngine;

fn replace_book_cell(
    samples: usize,
    scenario: &'static str,
    new_price: i64,
    new_qty: u64,
    out: &mut Vec<BenchRecord>,
) {
    const WARMUP: usize = 32;
    if samples == 0 {
        return;
    }
    let mut latencies = vec![0_u64; samples];
    let mut checksum = 0_u64;
    let mut next_id = 1_000_000_u64;
    let reject_unknown = scenario == "replace_reject_unknown";
    for step in 0..WARMUP + samples {
        let mut book = Box::new(OrderBook::<128, 8>::new(INSTRUMENT));
        let mut reports = ReportBuffer::<16>::new();
        let maker_id = take_id(&mut next_id);
        book.submit(
            order(
                maker_id,
                MAKER_ACCOUNT,
                100,
                10,
                Side::Sell,
                TimeInForce::Gtc,
            ),
            &mut reports,
        )
        .expect("fixture maker rests");
        reports.clear();
        let replace = ReplaceOrder {
            order_id: OrderId(maker_id + u64::from(reject_unknown)),
            account_id: MAKER_ACCOUNT,
            instrument_id: INSTRUMENT,
            sequence: SequenceNumber(maker_id),
            price: PriceTicks(new_price),
            quantity: Quantity(new_qty),
        };
        let before = snapshot_gate();
        let started = Instant::now();
        let result = black_box(&mut book).replace(black_box(replace));
        black_box(&result);
        let elapsed_ns = started.elapsed().as_nanos();
        assert_gate(before, snapshot_gate(), scenario);
        let contribution = if reject_unknown {
            assert_eq!(result, Err(RejectReason::UnknownOrder));
            0x9e
        } else {
            let replaced = result.expect("replace succeeds");
            assert_eq!(replaced.new_quantity, Quantity(new_qty));
            assert_eq!(replaced.price, PriceTicks(new_price));
            replaced.new_quantity.0
        };
        if step >= WARMUP {
            latencies[step - WARMUP] = u64::try_from(elapsed_ns).unwrap_or(u64::MAX);
            checksum = checksum.wrapping_add(contribution);
        }
        assert_eq!(book.order_count(), 1, "maker still rests");
    }
    finish(
        out,
        BenchRecord::new(
            "component",
            "book",
            scenario,
            &[("tif", Extra::Text("replace"))],
        ),
        checksum,
        &mut latencies,
    );
}
/// Risk-only reservation adjustments at fixed totals (alternating 1 <-> 4).
fn replace_risk_cell(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    let mut risk = RiskEngine::<2, 64>::new();
    let limits = hft_risk::RiskLimits {
        max_quantity: Quantity(16),
        max_notional: 1_000_000_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 32,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(10_000),
    };
    risk.register_account(AccountId(1), limits)
        .expect("risk cell account");
    let seed_id = 500_u64;
    risk.check_and_reserve(hft_types::NewOrder {
        time_in_force: hft_types::TimeInForce::Gtc,
        order_id: OrderId(seed_id),
        account_id: AccountId(1),
        instrument_id: InstrumentId(1),
        price: PriceTicks(100),
        quantity: Quantity(1),
        sequence: SequenceNumber(seed_id),
        side: Side::Sell,
    })
    .expect("seed reservation");
    let mut latencies = vec![0_u64; samples];
    let mut checksum = 0_u64;
    let before = snapshot_gate();
    for (index, sample) in latencies.iter_mut().enumerate() {
        let target = if index % 2 == 0 { 4_u64 } else { 1_u64 };
        let started = Instant::now();
        let adjusted = risk.adjust_reservation(
            OrderId(seed_id),
            AccountId(1),
            PriceTicks(100),
            Quantity(target),
        );
        let elapsed_ns = started.elapsed().as_nanos();
        if let Ok((released, _)) = adjusted {
            checksum ^= released.0;
        }
        *sample = u64::try_from(elapsed_ns).unwrap_or(u64::MAX);
    }
    assert_gate(
        before,
        (
            ALLOCATIONS.load(Ordering::SeqCst),
            DEALLOCATIONS.load(Ordering::SeqCst),
        ),
        "replace_risk_adjust",
    );
    finish(
        out,
        BenchRecord::new(
            "component",
            "risk",
            "replace_risk_adjust",
            &[("tif", Extra::Text("replace"))],
        ),
        checksum,
        &mut latencies,
    );
}

pub fn replace_benchmarks(samples: usize, out: &mut Vec<BenchRecord>) {
    replace_book_cell(samples, "replace_reduce", 100, 5, out);
    replace_book_cell(samples, "replace_increase", 100, 12, out);
    replace_book_cell(samples, "replace_reprice", 101, 10, out);
    replace_book_cell(samples, "replace_reject_unknown", 99, 5, out);
    replace_risk_cell(samples, out);
}
