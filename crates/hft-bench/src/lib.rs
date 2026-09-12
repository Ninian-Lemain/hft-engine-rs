//! Reproducible benchmark suite over the engine crates. Every cell emits one
//! [`record::BenchRecord`] serialized as a deterministic JSON line with
//! warm-up separated from sampling, allocation gates, and stable checksums.
#![forbid(unsafe_code)]

pub mod engine_workloads;
pub mod event_workloads;
pub mod extra_workloads;
pub mod journal_workloads;
pub mod liquidity_workloads;
pub mod record;
pub mod recovery_workloads;
pub mod router_workloads;
pub mod session_workloads;
pub mod tif_workloads;

use crate::record::{BenchRecord, Extra};
use hft_book::OrderBook;
use hft_gateway::Gateway;
use hft_io::RxFrame;
use hft_risk::{RiskEngine, RiskLimits};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, RejectReason,
    ReportBuffer, SequenceNumber, Side,
};
use hft_wire::encode_new_order;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// The counting allocator itself registers in the benchmark binary; these
// counters are its shared side effects so library gates can read them.
pub static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
pub static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);

/// Sample budgets for one suite pass. `full` matches released evidence;
/// `reduced` keeps structure and checksums intact for schema validation.
#[derive(Clone, Copy, Debug)]
pub struct SuiteConfig {
    pub gateway_iterations: u64,
    pub gateway_samples: usize,
    pub gateway_sample_after: u64,
    pub cancel_batches: u64,
    pub fifo_samples: usize,
    pub risk_samples: usize,
    pub risk_warmup: u64,
    pub price_samples: usize,
    pub plan_samples: usize,
    pub parser_samples: usize,
    pub spsc_samples: usize,
    pub mixed_commands: u64,
    pub mixed_warmup: u64,
    pub deep_samples: usize,
    pub tif_samples: usize,
}

impl SuiteConfig {
    #[must_use]
    pub const fn full() -> Self {
        Self {
            gateway_iterations: 100_000,
            gateway_samples: 2_000,
            gateway_sample_after: 1_000,
            cancel_batches: 1_024,
            fifo_samples: 2_000,
            risk_samples: 256,
            risk_warmup: 64,
            price_samples: 2_000,
            plan_samples: 2_000,
            parser_samples: 2_000,
            spsc_samples: 2_000,
            mixed_commands: 20_000,
            mixed_warmup: 1_000,
            deep_samples: 2_000,
            tif_samples: 2_000,
        }
    }

    #[must_use]
    pub const fn reduced() -> Self {
        Self {
            gateway_iterations: 2_000,
            gateway_samples: 100,
            gateway_sample_after: 100,
            cancel_batches: 4,
            fifo_samples: 100,
            risk_samples: 32,
            risk_warmup: 8,
            price_samples: 100,
            plan_samples: 100,
            parser_samples: 200,
            spsc_samples: 200,
            mixed_commands: 600,
            mixed_warmup: 100,
            deep_samples: 96,
            tif_samples: 96,
        }
    }
}

/// Runs every workload once and returns one JSON line per record, in a fixed
/// component order: gateway, cancel sweep, FIFO depths, risk, price shapes,
/// match plans, parser, SPSC, seeded mix, and deep-book sweeps.
#[must_use]
pub fn run_suite(config: SuiteConfig) -> std::vec::Vec<std::string::String> {
    // Reserved up front so record collection never allocates inside a
    // measured region.
    let mut records = std::vec::Vec::with_capacity(64);
    gateway_benchmark(config, &mut records);
    cancel_benchmark(config, &mut records);
    fifo_benchmark(config, &mut records);
    risk_benchmark(config, &mut records);
    price_benchmark(config, &mut records);
    match_plan_benchmark(config, &mut records);
    liquidity_workloads::liquidity_benchmarks(config.fifo_samples, &mut records);
    extra_workloads::parser_benchmark(config.parser_samples, &mut records);
    extra_workloads::spsc_benchmark(config.spsc_samples, 0x0a7c_bee5, &mut records);
    extra_workloads::gateway_mixed_benchmark(
        config.mixed_commands,
        config.mixed_warmup,
        0x0a7c_6dd5,
        &mut records,
    );
    extra_workloads::deep_book_benchmark(config.deep_samples, 0x0a7c_dee1, &mut records);
    tif_workloads::tif_benchmark(config.tif_samples, &mut records);
    tif_workloads::replace_benchmarks(config.tif_samples, &mut records);
    session_workloads::session_benchmark(config.tif_samples, &mut records);
    session_workloads::recovery_benchmark(config.tif_samples, &mut records);
    journal_workloads::journal_checksum_benchmarks(config.tif_samples, &mut records);
    journal_workloads::journal_benchmark(config.tif_samples, &mut records);
    journal_workloads::journal_drain_benchmark(config.tif_samples, &mut records);
    journal_workloads::controlled_journal_benchmarks(config.tif_samples, &mut records);
    recovery_workloads::recovery_benchmarks(config.tif_samples, &mut records);
    event_workloads::event_benchmarks(config.tif_samples, &mut records);
    router_workloads::router_benchmarks(config.tif_samples, &mut records);
    engine_workloads::engine_benchmarks(config.tif_samples, &mut records);
    records.iter().map(BenchRecord::to_json_line).collect()
}

fn push_latency_record(
    out: &mut std::vec::Vec<BenchRecord>,
    mut record: BenchRecord,
    samples: &mut [u64],
) {
    let stats = analyze(samples);
    record.samples = samples.len();
    record.mean_ns = u64::try_from(stats.mean).unwrap_or(u64::MAX);
    record.p50_ns = stats.p50;
    record.p90_ns = stats.p90;
    record.p99_ns = stats.p99;
    record.p99_9_ns = stats.p99_9;
    record.max_ns = stats.max;
    record.ops_per_second = 1_000_000_000_u64.checked_div(record.mean_ns).unwrap_or(0);
    out.push(record);
}

fn allocation_gate() -> (u64, u64) {
    let allocations = ALLOCATIONS.load(Ordering::SeqCst);
    let deallocations = DEALLOCATIONS.load(Ordering::SeqCst);
    (allocations, deallocations)
}

fn assert_allocation_gate(before: (u64, u64), context: &'static str) {
    assert_eq!(
        ALLOCATIONS.load(Ordering::SeqCst),
        before.0,
        "{context}: allocations"
    );
    assert_eq!(
        DEALLOCATIONS.load(Ordering::SeqCst),
        before.1,
        "{context}: deallocations"
    );
}

/// Packet-to-report gateway loop. Sampling skips the first iterations so
/// cold-start effects stay out of the reported distribution; the message
/// sequence is identical either way, so the digest stays comparable.
#[allow(clippy::too_many_lines)]
fn gateway_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    let limits = RiskLimits {
        max_quantity: Quantity(10),
        max_notional: 10_000,
        max_abs_position: Quantity(config.gateway_iterations + 1),
        max_open_orders: 8,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    };
    let mut risk = RiskEngine::<2, 8>::new();
    risk.register_account(AccountId(1), limits)
        .expect("benchmark account one");
    risk.register_account(AccountId(2), limits)
        .expect("benchmark account two");
    let mut gateway = Gateway::<2, 8, 2, 2>::new(risk, InstrumentId(1));
    let mut reports = ReportBuffer::<1>::new();

    run_pair(&mut gateway, &mut reports, 1).expect("warm-up path");
    let gate = allocation_gate();
    let mut samples = [0_u64; 2_000];
    let sample_count = config.gateway_samples.min(samples.len());
    for iteration in 0..config.gateway_iterations {
        let first_id = iteration * 2 + 3;
        let sample_window = iteration >= config.gateway_sample_after
            && iteration < config.gateway_sample_after + sample_count as u64 / 2;
        if sample_window {
            let sample =
                usize::try_from(iteration - config.gateway_sample_after).expect("sample index");
            let sell = encode_new_order(order(first_id, AccountId(1), Side::Sell));
            let sample_started = Instant::now();
            gateway
                .process_frame(&RxFrame::from_bytes(&sell), &mut reports)
                .expect("sampled sell");
            samples[sample * 2] =
                u64::try_from(sample_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

            let buy = encode_new_order(order(first_id + 1, AccountId(2), Side::Buy));
            let sample_started = Instant::now();
            gateway
                .process_frame(&RxFrame::from_bytes(&buy), &mut reports)
                .expect("sampled buy");
            samples[sample * 2 + 1] =
                u64::try_from(sample_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        } else {
            run_pair(&mut gateway, &mut reports, first_id).expect("measured path");
        }
    }
    assert_allocation_gate(gate, "gateway hot path");
    let messages = config.gateway_iterations * 2;
    let digest = gateway.stable_digest();

    push_latency_record(
        out,
        BenchRecord {
            checksum: digest,
            ..BenchRecord::new(
                "gateway",
                "gateway",
                "pair_rest_fill",
                &[("messages", Extra::U64(messages))],
            )
        },
        &mut samples[..sample_count],
    );
    let (maximum_occupancy, backpressure_events) = queue_capacity_smoke();
    out.push(BenchRecord {
        checksum: u64::try_from(maximum_occupancy).unwrap_or(u64::MAX),
        samples: 65,
        ..BenchRecord::new(
            "component",
            "queue",
            "capacity_smoke",
            &[
                ("capacity", Extra::U64(64)),
                (
                    "max_occupancy",
                    Extra::U64(u64::try_from(maximum_occupancy).unwrap_or(u64::MAX)),
                ),
                (
                    "backpressure_events",
                    Extra::U64(u64::try_from(backpressure_events).unwrap_or(u64::MAX)),
                ),
            ],
        )
    });
}

#[derive(Clone, Copy)]
enum FifoScenario {
    HeadCancel,
    MiddleCancel,
    TailCancel,
    HeadFill,
}

impl FifoScenario {
    const ALL: [Self; 4] = [
        Self::HeadCancel,
        Self::MiddleCancel,
        Self::TailCancel,
        Self::HeadFill,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::HeadCancel => "head_cancel",
            Self::MiddleCancel => "middle_cancel",
            Self::TailCancel => "tail_cancel",
            Self::HeadFill => "head_fill",
        }
    }
}

fn fifo_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    for scenario in FifoScenario::ALL {
        run_fifo_cell::<1>(config, scenario, out);
        run_fifo_cell::<4>(config, scenario, out);
        run_fifo_cell::<16>(config, scenario, out);
        run_fifo_cell::<64>(config, scenario, out);
        run_fifo_cell::<512>(config, scenario, out);
    }
}

/// One timed fifo operation on a pre-built book. Book construction stays
/// outside the timed region.
fn fifo_op<const DEPTH: usize>(
    book: &mut OrderBook<1, DEPTH>,
    reports: &mut ReportBuffer<1>,
    scenario: FifoScenario,
    target: u64,
) -> u64 {
    match scenario {
        FifoScenario::HeadCancel | FifoScenario::MiddleCancel | FifoScenario::TailCancel => {
            let cancelled = book
                .cancel(CancelOrder {
                    order_id: OrderId(target),
                    account_id: AccountId(1),
                    instrument_id: InstrumentId(1),
                    sequence: SequenceNumber(target),
                })
                .expect("fifo cancel");
            cancelled.order_id.0
        }
        FifoScenario::HeadFill => {
            let taker = u64::try_from(DEPTH).expect("depth fits u64") + 1;
            let summary = book
                .submit(
                    NewOrder {
                        time_in_force: hft_types::TimeInForce::Gtc,
                        order_id: OrderId(taker),
                        account_id: AccountId(2),
                        instrument_id: InstrumentId(1),
                        price: PriceTicks(100),
                        quantity: Quantity(1),
                        sequence: SequenceNumber(taker),
                        side: Side::Buy,
                    },
                    reports,
                )
                .expect("fifo head fill");
            debug_assert_eq!(summary.filled_quantity, Quantity(1));
            reports
                .iter()
                .next()
                .map_or(0, |report| report.maker_order_id.0)
        }
    }
}

fn run_fifo_cell<const DEPTH: usize>(
    config: SuiteConfig,
    scenario: FifoScenario,
    out: &mut std::vec::Vec<BenchRecord>,
) {
    const WARMUP: usize = 64;
    let gate = allocation_gate();
    let mut samples_ns = [0_u64; 2_000];
    let sample_count = config.fifo_samples.min(samples_ns.len());
    let mut checksum = 0_u64;
    let target = match scenario {
        FifoScenario::HeadCancel | FifoScenario::HeadFill => 1,
        FifoScenario::MiddleCancel => DEPTH.div_ceil(2),
        FifoScenario::TailCancel => DEPTH,
    };
    let target = u64::try_from(target).expect("target id fits u64");
    for _ in 0..WARMUP {
        let mut book = fifo_fixture::<DEPTH>();
        let mut reports = ReportBuffer::<1>::new();
        checksum ^= fifo_op(&mut book, &mut reports, scenario, target);
    }
    for sample in &mut samples_ns[..sample_count] {
        let mut book = fifo_fixture::<DEPTH>();
        let mut reports = ReportBuffer::<1>::new();
        let started = Instant::now();
        let contribution = fifo_op(&mut book, &mut reports, scenario, target);
        *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        checksum ^= contribution;
    }
    assert_allocation_gate(gate, "fifo");
    let book_bytes = u64::try_from(core::mem::size_of::<OrderBook<1, DEPTH>>()).unwrap_or(u64::MAX);
    push_latency_record(
        out,
        BenchRecord {
            checksum,
            ..BenchRecord::new(
                "component",
                "book",
                scenario.name(),
                &[
                    (
                        "depth",
                        Extra::U64(u64::try_from(DEPTH).unwrap_or(u64::MAX)),
                    ),
                    ("book_bytes", Extra::U64(book_bytes)),
                ],
            )
        },
        &mut samples_ns[..sample_count],
    );
}

fn fifo_fixture<const DEPTH: usize>() -> OrderBook<1, DEPTH> {
    let mut book = OrderBook::<1, DEPTH>::new(InstrumentId(1));
    let mut reports = ReportBuffer::<1>::new();
    for id in 1..=u64::try_from(DEPTH).expect("depth fits u64") {
        book.submit(
            NewOrder {
                time_in_force: hft_types::TimeInForce::Gtc,
                order_id: OrderId(id),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                price: PriceTicks(100),
                quantity: Quantity(1),
                sequence: SequenceNumber(id),
                side: Side::Sell,
            },
            &mut reports,
        )
        .expect("fixture order rests");
        reports.clear();
    }
    book
}

/// Engine at the stated reservation occupancy with 60 registered accounts.
fn risk_fixture(occupancy: u64, limits: RiskLimits) -> RiskEngine<64, 1024> {
    let mut risk = RiskEngine::<64, 1024>::new();
    for account in 1..=60 {
        risk.register_account(AccountId(account), limits)
            .expect("register account");
    }
    for id in 1..=occupancy {
        let side = if id % 2 == 0 { Side::Sell } else { Side::Buy };
        risk.check_and_reserve(NewOrder {
            time_in_force: hft_types::TimeInForce::Gtc,
            order_id: OrderId(id),
            account_id: AccountId(u32::try_from((id % 60) + 1).expect("account id fits u32")),
            instrument_id: InstrumentId(1),
            price: PriceTicks(100),
            quantity: Quantity(1),
            sequence: SequenceNumber(id),
            side,
        })
        .expect("populate reservation");
    }
    risk
}

/// One reservation attempt shared by the risk benchmark loops.
fn reserve_one(
    risk: &mut RiskEngine<64, 1024>,
    id: u64,
    quantity: u64,
) -> Result<(), RejectReason> {
    let side = if id % 2 == 0 { Side::Sell } else { Side::Buy };
    risk.check_and_reserve(NewOrder {
        time_in_force: hft_types::TimeInForce::Gtc,
        order_id: OrderId(id),
        account_id: AccountId(u32::try_from((id % 60) + 1).expect("account id fits u32")),
        instrument_id: InstrumentId(1),
        price: PriceTicks(100),
        quantity: Quantity(quantity),
        sequence: SequenceNumber(id),
        side,
    })
}

fn push_risk_record(
    out: &mut std::vec::Vec<BenchRecord>,
    op: &'static str,
    index: &'static str,
    occupancy: u64,
    engine_bytes: u64,
    checksum: u64,
    samples: &mut [u64],
) {
    push_latency_record(
        out,
        BenchRecord {
            checksum,
            ..BenchRecord::new(
                "component",
                "risk",
                op,
                &[
                    ("index", Extra::Text(index)),
                    ("occupancy", Extra::U64(occupancy)),
                    ("engine_bytes", Extra::U64(engine_bytes)),
                ],
            )
        },
        samples,
    );
}

#[allow(clippy::too_many_lines)]
fn risk_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    const ACCOUNT_CAPACITY: usize = 64;
    const ORDER_CAPACITY: usize = 1024;

    let gate = allocation_gate();
    let wide = RiskLimits {
        max_quantity: Quantity(100_000),
        max_notional: 1_000_000_000_000,
        max_abs_position: Quantity(1_000_000),
        max_open_orders: 4096,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000_000),
    };
    let engine_bytes = u64::try_from(core::mem::size_of::<
        RiskEngine<ACCOUNT_CAPACITY, ORDER_CAPACITY>,
    >())
    .unwrap_or(u64::MAX);

    // Reservation occupancy sweeps: 102, 512, 921 (about 10 %, 50 %, 90 % of
    // 1024). Every operation runs against a freshly populated engine so
    // destructive operations measure live reservations.
    for &target in &[102_u64, 512, 921] {
        // risk_check: reserve fresh orders starting at the target occupancy.
        {
            let budget = ORDER_CAPACITY as u64 - target;
            let warm = config.risk_warmup.min(budget / 4);
            let op_samples = u64::try_from(config.risk_samples)
                .unwrap_or(u64::MAX)
                .min(budget - warm);
            let mut risk = risk_fixture(target, wide);
            let mut checksum = 0_u64;
            for i in 0..warm {
                reserve_one(&mut risk, target + i + 1, 1).expect("warm-up reservation");
                checksum ^= target + i + 1;
            }
            let measured = usize::try_from(op_samples).unwrap_or(usize::MAX);
            let mut samples = [0_u64; 256];
            time_samples(&mut samples[..measured], |i| {
                let id = target + warm + i + 1;
                if reserve_one(&mut risk, id, 1).is_ok() {
                    checksum ^= id;
                }
            });
            push_risk_record(
                out,
                "risk_check",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples[..measured],
            );
        }

        // reservation_lookup: probe live reservations without mutation.
        {
            let risk = risk_fixture(target, wide);
            let mut checksum = 0_u64;
            for i in 0..config.risk_warmup {
                let id = (i % target) + 1;
                let _ = risk.can_cancel(
                    OrderId(id),
                    AccountId(u32::try_from((id % 60) + 1).unwrap_or(1)),
                );
                checksum ^= id;
            }
            let mut samples = [0_u64; 256];
            time_samples(&mut samples, |i| {
                let id = (i % target) + 1;
                let found = risk.can_cancel(
                    OrderId(id),
                    AccountId(u32::try_from((id % 60) + 1).unwrap_or(1)),
                );
                debug_assert!(found.is_ok(), "lookup targets a live reservation");
                checksum ^= id;
            });
            push_risk_record(
                out,
                "reservation_lookup",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples,
            );
        }

        // fill, cancel, settle are destructive: each sample consumes one live
        // reservation, so the sample count is capped by the occupancy.
        let live_samples = config
            .risk_samples
            .min(usize::try_from(target).unwrap_or(usize::MAX));

        // fill: terminal one-unit fills of live reservations.
        {
            let mut risk = risk_fixture(target + config.risk_warmup, wide);
            let mut checksum = 0_u64;
            for i in 0..config.risk_warmup {
                let id = target + config.risk_warmup - i;
                risk.record_fill(OrderId(id), Quantity(1))
                    .expect("warm-up fill");
                checksum ^= id;
            }
            let mut samples = [0_u64; 256];
            time_samples(&mut samples[..live_samples], |i| {
                let id = target - i;
                if risk.record_fill(OrderId(id), Quantity(1)).is_ok() {
                    checksum ^= id;
                }
            });
            push_risk_record(
                out,
                "fill",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples[..live_samples],
            );
        }

        // cancel: release live reservations.
        {
            let mut risk = risk_fixture(target + config.risk_warmup, wide);
            let mut checksum = 0_u64;
            for i in 0..config.risk_warmup {
                let id = target + config.risk_warmup - i;
                risk.cancel_reservation(
                    OrderId(id),
                    AccountId(u32::try_from((id % 60) + 1).unwrap_or(1)),
                )
                .expect("warm-up cancel");
                checksum ^= id;
            }
            let mut samples = [0_u64; 256];
            time_samples(&mut samples[..live_samples], |i| {
                let id = target - i;
                let released = risk.cancel_reservation(
                    OrderId(id),
                    AccountId(u32::try_from((id % 60) + 1).unwrap_or(1)),
                );
                debug_assert!(released.is_ok(), "cancel targets a live reservation");
                checksum ^= id;
            });
            push_risk_record(
                out,
                "cancel",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples[..live_samples],
            );
        }

        // settle: settle live reservations with their full filled quantity.
        {
            let mut risk = risk_fixture(target + config.risk_warmup, wide);
            let mut checksum = 0_u64;
            for i in 0..config.risk_warmup {
                let id = target + config.risk_warmup - i;
                risk.settle(OrderId(id), Quantity(1))
                    .expect("warm-up settle");
                checksum ^= id;
            }
            let mut samples = [0_u64; 256];
            time_samples(&mut samples[..live_samples], |i| {
                let id = target - i;
                let settled = risk.settle(OrderId(id), Quantity(1));
                debug_assert!(settled.is_ok(), "settle targets a live reservation");
                checksum ^= id;
            });
            push_risk_record(
                out,
                "settle",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples[..live_samples],
            );
        }

        // reject: oversized quantity fails the limit check without mutation.
        {
            let mut risk = risk_fixture(target, wide);
            let mut checksum = 0_u64;
            for i in 0..config.risk_warmup {
                let _ = reserve_one(&mut risk, target + i + 20_000, 100_001);
                checksum ^= target + i + 20_000;
            }
            let mut samples = [0_u64; 256];
            time_samples(&mut samples, |i| {
                let id = target + i + 30_000;
                let rejected = reserve_one(&mut risk, id, 100_001);
                debug_assert_eq!(rejected, Err(RejectReason::QuantityLimit));
                checksum ^= id;
            });
            push_risk_record(
                out,
                "reject",
                "reservation",
                target,
                engine_bytes,
                checksum,
                &mut samples,
            );
        }
    }

    // Account index occupancy sweeps: 6, 32, 57 (about 10 %, 50 %, 90 % of
    // 64).
    for &target in &[6_u64, 32, 57] {
        let mut risk = RiskEngine::<ACCOUNT_CAPACITY, ORDER_CAPACITY>::new();
        for acct in 1..=target {
            risk.register_account(AccountId(u32::try_from(acct).unwrap_or(1)), wide)
                .expect("register account");
        }
        let mut checksum = 0_u64;
        for i in 0..config.risk_warmup {
            let acct = (i % target) + 1;
            let _ = risk.account_snapshot(AccountId(u32::try_from(acct).unwrap_or(1)));
            checksum ^= acct;
        }
        let mut samples = [0_u64; 256];
        time_samples(&mut samples, |i| {
            let acct = (i % target) + 1;
            let _ = risk.account_snapshot(AccountId(u32::try_from(acct).unwrap_or(1)));
            checksum ^= acct;
        });
        push_risk_record(
            out,
            "account_lookup",
            "account",
            target,
            engine_bytes,
            checksum,
            &mut samples,
        );
    }

    assert_allocation_gate(gate, "risk bench");
}

fn bench_order(id: u64, account: u32, price: i64, quantity: u64, side: Side) -> NewOrder {
    NewOrder {
        time_in_force: hft_types::TimeInForce::Gtc,
        order_id: OrderId(id),
        account_id: AccountId(account),
        instrument_id: InstrumentId(1),
        price: PriceTicks(price),
        quantity: Quantity(quantity),
        sequence: SequenceNumber(id),
        side,
    }
}

fn price_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    let gate = allocation_gate();

    price_scenario::<64, 1>(config, "dense_64_32", 32, out);
    price_scenario::<128, 1>(config, "sparse_128_16", 16, out);
    price_scenario::<128, 1>(config, "dense_128_64", 64, out);
    price_scenario::<128, 1>(config, "dense_128_120", 120, out);

    assert_allocation_gate(gate, "price bench");
}

/// Every scenario keeps its book in a steady state: each timed operation is
/// balanced by an untimed operation that restores the starting shape, so all
/// samples measure the intended operation at the stated occupancy.
#[allow(clippy::too_many_lines)]
fn price_scenario<const L: usize, const O: usize>(
    config: SuiteConfig,
    name: &'static str,
    active: usize,
    out: &mut std::vec::Vec<BenchRecord>,
) {
    const WARMUP: u64 = 64;
    let shape = [("shape", Extra::Text(name))];

    // submit_cross: a one-unit buy crosses the best ask; the consumed maker
    // is replaced untimed, so every sample crosses a full book.
    {
        let mut book = OrderBook::<L, O>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        for i in 0..active {
            let id = u64::try_from(i + 1).unwrap_or(u64::MAX);
            let price = 100 + i64::try_from(i).unwrap_or(i64::MAX);
            book.submit(bench_order(id, 1, price, 1, Side::Sell), &mut reports)
                .expect("rest sell level");
            reports.clear();
        }
        let primary = |book: &mut OrderBook<L, O>, reports: &mut ReportBuffer<1>, i: u64| {
            let summary = book
                .submit(bench_order(1_000_000 + i, 2, 1_000, 1, Side::Buy), reports)
                .expect("cross best ask");
            debug_assert_eq!(summary.filled_quantity, Quantity(1));
            let consumed_price = reports.iter().next().expect("one fill").price.0;
            reports.clear();
            consumed_price
        };
        let teardown =
            |book: &mut OrderBook<L, O>, reports: &mut ReportBuffer<1>, i: u64, price: i64| {
                book.submit(bench_order(2_000_000 + i, 1, price, 1, Side::Sell), reports)
                    .expect("replenish consumed level");
                reports.clear();
            };
        for i in 0..WARMUP {
            let price = primary(&mut book, &mut reports, i);
            teardown(&mut book, &mut reports, i, price);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.price_samples.min(samples.len());
        for (index, sample) in samples[..sample_count].iter_mut().enumerate() {
            let i = WARMUP + u64::try_from(index).unwrap_or(u64::MAX);
            let started = Instant::now();
            let price = primary(&mut book, &mut reports, i);
            *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            teardown(&mut book, &mut reports, i, price);
        }
        push_named_record(out, "submit_cross", &shape, &mut samples[..sample_count]);
    }

    // discovery: deep makers never deplete, so a one-unit buy at the best
    // price measures pure best-price discovery plus a partial fill.
    {
        let mut book = OrderBook::<L, O>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        for i in 0..active {
            let id = u64::try_from(i + 1).unwrap_or(u64::MAX);
            let price = 100 + i64::try_from(i).unwrap_or(i64::MAX);
            book.submit(bench_order(id, 1, price, 100_000, Side::Sell), &mut reports)
                .expect("rest sell");
            reports.clear();
        }
        let discover = |book: &mut OrderBook<L, O>, reports: &mut ReportBuffer<1>, i: u64| {
            let taker = bench_order(1_000_000 + i, 2, 100, 1, Side::Buy);
            let summary = book.submit(taker, reports).expect("discover best ask");
            debug_assert_eq!(summary.filled_quantity, Quantity(1));
            reports.clear();
        };
        for i in 0..WARMUP {
            discover(&mut book, &mut reports, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.price_samples.min(samples.len());
        time_samples(&mut samples[..sample_count], |i| {
            discover(&mut book, &mut reports, WARMUP + i);
        });
        push_named_record(out, "discovery", &shape, &mut samples[..sample_count]);
    }

    // level_create: a timed rest into a new price level, then an untimed
    // cancel returns the slot, so every sample creates a level at the same
    // occupancy.
    {
        let mut book = OrderBook::<L, O>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        for i in 0..active.min(8) {
            let id = u64::try_from(i + 1).unwrap_or(u64::MAX);
            let price = 200 + i64::try_from(i).unwrap_or(i64::MAX);
            book.submit(bench_order(id, 1, price, 1, Side::Sell), &mut reports)
                .expect("pre-populate");
            reports.clear();
        }
        let primary = |book: &mut OrderBook<L, O>, reports: &mut ReportBuffer<1>, i: u64| {
            let id = 3_000_000 + i;
            book.submit(bench_order(id, 1, 500, 1, Side::Sell), reports)
                .expect("create level");
            reports.clear();
        };
        let teardown = |book: &mut OrderBook<L, O>, i: u64| {
            let id = 3_000_000 + i;
            book.cancel(CancelOrder {
                order_id: OrderId(id),
                account_id: AccountId(1),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(id),
            })
            .expect("remove created level");
        };
        for i in 0..WARMUP {
            primary(&mut book, &mut reports, i);
            teardown(&mut book, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.price_samples.min(samples.len());
        for (index, sample) in samples[..sample_count].iter_mut().enumerate() {
            let i = WARMUP + u64::try_from(index).unwrap_or(u64::MAX);
            let started = Instant::now();
            primary(&mut book, &mut reports, i);
            *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            teardown(&mut book, i);
        }
        push_named_record(out, "level_create", &shape, &mut samples[..sample_count]);
    }
}

fn push_named_record(
    out: &mut std::vec::Vec<BenchRecord>,
    scenario: &'static str,
    params: &[(&'static str, Extra)],
    samples: &mut [u64],
) {
    push_latency_record(
        out,
        BenchRecord::new("component", "book", scenario, params),
        samples,
    );
}

#[allow(clippy::too_many_lines)]
fn match_plan_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    const WARMUP: u64 = 64;
    let gate = allocation_gate();
    let shape = [
        ("levels", Extra::U64(128)),
        ("orders_per_level", Extra::U64(8)),
    ];

    // Non-crossing: taker rests without walking the plan; the rest is
    // cancelled untimed so the book shape is constant across samples.
    {
        let mut book = OrderBook::<128, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        book.submit(bench_order(1, 1, 100, 10_000, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        let mut checksum = 0_u64;
        let primary = |book: &mut OrderBook<128, 8>, reports: &mut ReportBuffer<8>, i: u64| {
            let summary = book
                .submit(bench_order(1_000_000 + i, 2, 99, 1, Side::Buy), reports)
                .expect("non-crossing rests");
            reports.clear();
            summary.filled_quantity.0
        };
        let teardown = |book: &mut OrderBook<128, 8>, i: u64| {
            let id = 1_000_000 + i;
            book.cancel(CancelOrder {
                order_id: OrderId(id),
                account_id: AccountId(2),
                instrument_id: InstrumentId(1),
                sequence: SequenceNumber(id),
            })
            .expect("rested bid cancels");
        };
        for i in 0..WARMUP {
            checksum ^= primary(&mut book, &mut reports, i);
            teardown(&mut book, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.plan_samples.min(samples.len());
        for (index, sample) in samples[..sample_count].iter_mut().enumerate() {
            let i = WARMUP + u64::try_from(index).unwrap_or(u64::MAX);
            let started = Instant::now();
            checksum ^= primary(&mut book, &mut reports, i);
            *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            teardown(&mut book, i);
        }
        push_plan_record(
            out,
            "non_crossing",
            "traversals=0 fills=0 reports=0",
            &shape,
            checksum,
            &mut samples[..sample_count],
        );
    }

    // Single fill: one deep maker crosses at the best price and never
    // depletes across the sample count.
    {
        let mut book = OrderBook::<128, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        book.submit(bench_order(1, 1, 100, 10_000_000, Side::Sell), &mut reports)
            .expect("rest ask");
        reports.clear();
        let mut checksum = 0_u64;
        let mut fill_once =
            |book: &mut OrderBook<128, 8>, reports: &mut ReportBuffer<8>, i: u64| {
                let summary = book
                    .submit(bench_order(1_000_000 + i, 2, 100, 1, Side::Buy), reports)
                    .expect("single fill");
                checksum ^= summary.filled_quantity.0;
                reports.clear();
            };
        for i in 0..WARMUP {
            fill_once(&mut book, &mut reports, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.plan_samples.min(samples.len());
        time_samples(&mut samples[..sample_count], |i| {
            fill_once(&mut book, &mut reports, WARMUP + i);
        });
        push_plan_record(
            out,
            "single_fill",
            "traversals=1 fills=1 reports=1",
            &shape,
            checksum,
            &mut samples[..sample_count],
        );
    }

    // Multi fill: eight single-unit makers across eight levels; the taker
    // consumes all eight, and the untimed teardown replenishes them so every
    // sample traverses the same depth.
    {
        let mut book = OrderBook::<128, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        let mut next_maker_id = 1_500_000_u64;
        let rest_fixtures = |book: &mut OrderBook<128, 8>,
                             reports: &mut ReportBuffer<8>,
                             next_maker_id: &mut u64| {
            for i in 0..8_u64 {
                let price = 100 + i64::try_from(i).unwrap_or(i64::MAX);
                let id = *next_maker_id;
                *next_maker_id += 1;
                book.submit(bench_order(id, 1, price, 1, Side::Sell), reports)
                    .expect("rest ask maker");
                reports.clear();
            }
        };
        rest_fixtures(&mut book, &mut reports, &mut next_maker_id);
        let mut checksum = 0_u64;
        let mut cross_eight =
            |book: &mut OrderBook<128, 8>, reports: &mut ReportBuffer<8>, i: u64| {
                let summary = book
                    .submit(bench_order(1_000_000 + i, 2, 1_000, 8, Side::Buy), reports)
                    .expect("multi fill");
                debug_assert_eq!(summary.report_count, 8, "one report per maker");
                checksum ^= summary.filled_quantity.0;
                reports.clear();
            };
        for i in 0..WARMUP {
            cross_eight(&mut book, &mut reports, i);
            rest_fixtures(&mut book, &mut reports, &mut next_maker_id);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.plan_samples.min(samples.len());
        for (index, sample) in samples[..sample_count].iter_mut().enumerate() {
            let i = WARMUP + u64::try_from(index).unwrap_or(u64::MAX);
            let started = Instant::now();
            cross_eight(&mut book, &mut reports, i);
            *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            rest_fixtures(&mut book, &mut reports, &mut next_maker_id);
        }
        push_plan_record(
            out,
            "multi_fill",
            "traversals=8 fills=8 reports=8",
            &shape,
            checksum,
            &mut samples[..sample_count],
        );
    }

    // Report-full rejection: a taker that would exceed report capacity is
    // rejected atomically by the plan preflight.
    {
        let mut book = OrderBook::<128, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        for i in 0..9_u64 {
            let price = 100 + i64::try_from(i).unwrap_or(i64::MAX);
            book.submit(bench_order(i + 1, 1, price, 1, Side::Sell), &mut reports)
                .expect("rest ask level");
            reports.clear();
        }
        let reject_once = |book: &mut OrderBook<128, 8>, reports: &mut ReportBuffer<8>, i: u64| {
            let rejected = book
                .submit(bench_order(1_000_000 + i, 2, 1_000, 9, Side::Buy), reports)
                .expect_err("report capacity rejected");
            debug_assert!(matches!(rejected, RejectReason::ReportCapacity));
        };
        for i in 0..WARMUP {
            reject_once(&mut book, &mut reports, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.plan_samples.min(samples.len());
        time_samples(&mut samples[..sample_count], |i| {
            reject_once(&mut book, &mut reports, WARMUP + i);
        });
        push_plan_record(
            out,
            "report_full",
            "traversals=9 fills=0 reports=0",
            &shape,
            0,
            &mut samples[..sample_count],
        );
    }

    // Deep rejection: a taker trying to rest into a full best-priced level is
    // rejected without mutating the book.
    {
        let mut book = OrderBook::<128, 8>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<8>::new();
        for i in 0..8_u64 {
            book.submit(bench_order(i + 1, 1, 100, 1, Side::Sell), &mut reports)
                .expect("rest ask at best price");
            reports.clear();
        }
        let reject_once = |book: &mut OrderBook<128, 8>, reports: &mut ReportBuffer<8>, i: u64| {
            let rejected = book
                .submit(bench_order(1_000_000 + i, 1, 100, 1, Side::Sell), reports)
                .expect_err("full best level rejected");
            debug_assert!(matches!(rejected, RejectReason::PriceLevelOrderCapacity));
        };
        for i in 0..WARMUP {
            reject_once(&mut book, &mut reports, i);
        }
        let mut samples = [0_u64; 2_000];
        let sample_count = config.plan_samples.min(samples.len());
        time_samples(&mut samples[..sample_count], |i| {
            reject_once(&mut book, &mut reports, WARMUP + i);
        });
        push_plan_record(
            out,
            "deep_rejection",
            "traversals=1 fills=0 reports=0",
            &shape,
            0,
            &mut samples[..sample_count],
        );
    }

    assert_allocation_gate(gate, "match plan bench");
}

fn push_plan_record(
    out: &mut std::vec::Vec<BenchRecord>,
    scenario: &'static str,
    shape_text: &'static str,
    params: &[(&'static str, Extra)],
    checksum: u64,
    samples: &mut [u64],
) {
    assert_eq!(params.len(), 2, "plan shape carries two entries");
    let combined = [params[0], params[1], ("plan", Extra::Text(shape_text))];
    push_latency_record(
        out,
        BenchRecord {
            checksum,
            ..BenchRecord::new("component", "book", scenario, &combined)
        },
        samples,
    );
}

fn cancel_benchmark(config: SuiteConfig, out: &mut std::vec::Vec<BenchRecord>) {
    const LEVELS: usize = 512;
    let levels = u64::try_from(LEVELS).unwrap_or(u64::MAX);
    let gate = allocation_gate();
    let mut samples_ns = [0_u64; 1_024];
    let batch_sample_count = usize::try_from(config.cancel_batches.min(1_024)).unwrap_or(1_024);
    let mut checksum = 0_u64;

    // Batch 0 is an untimed warm-up; later batches contribute one sample of
    // nanoseconds per cancel.
    for batch in 0..=config.cancel_batches {
        let mut book = OrderBook::<LEVELS, 1>::new(InstrumentId(1));
        let mut reports = ReportBuffer::<1>::new();
        let first_id = batch * levels + 1;
        for level in 0..LEVELS {
            let offset = u64::try_from(level).unwrap_or(u64::MAX);
            let id = first_id + offset;
            let price = i64::try_from(level + 1).unwrap_or(i64::MAX);
            book.submit(bench_order(id, 1, price, 1, Side::Sell), &mut reports)
                .expect("benchmark order rests");
            reports.clear();
        }

        let started = Instant::now();
        let mut batch_checksum = 0_u64;
        for level in (0..LEVELS).rev() {
            let id = first_id + u64::try_from(level).unwrap_or(u64::MAX);
            let cancelled = book
                .cancel(CancelOrder {
                    order_id: OrderId(id),
                    account_id: AccountId(1),
                    instrument_id: InstrumentId(1),
                    sequence: SequenceNumber(id),
                })
                .expect("benchmark cancellation");
            batch_checksum ^= cancelled.order_id.0;
        }
        let batch_ns = started.elapsed().as_nanos();
        assert_eq!(book.order_count(), 0, "all benchmark orders cancelled");
        if batch > 0 {
            checksum ^= batch_checksum;
            let batch_index = usize::try_from(batch - 1).unwrap_or(usize::MAX);
            if batch_index < batch_sample_count {
                samples_ns[batch_index] =
                    u64::try_from(batch_ns / u128::from(levels)).unwrap_or(u64::MAX);
            }
        }
    }

    assert_allocation_gate(gate, "cancel bench");
    let book_bytes =
        u64::try_from(core::mem::size_of::<OrderBook<LEVELS, 1>>()).unwrap_or(u64::MAX);
    push_latency_record(
        out,
        BenchRecord {
            checksum,
            ..BenchRecord::new(
                "component",
                "book",
                "cancel_sweep",
                &[
                    ("levels", Extra::U64(levels)),
                    ("orders_per_level", Extra::U64(1)),
                    ("book_bytes", Extra::U64(book_bytes)),
                ],
            )
        },
        &mut samples_ns[..batch_sample_count],
    );
}

fn queue_capacity_smoke() -> (usize, usize) {
    let mut queue = SpscQueue::<u64, 64>::try_new().expect("valid queue capacity");
    let (mut producer, mut consumer) = queue.split();
    let mut occupancy = 0_usize;
    let mut maximum = 0_usize;
    let mut backpressure = 0_usize;
    for value in 0..65 {
        if producer.try_push(value).is_ok() {
            occupancy += 1;
            maximum = maximum.max(occupancy);
        } else {
            backpressure += 1;
        }
    }
    while consumer.try_pop().is_some() {
        occupancy -= 1;
    }
    debug_assert_eq!(occupancy, 0);
    (maximum, backpressure)
}

fn percentile(samples: &[u64], permille: usize) -> u64 {
    let index = samples
        .len()
        .saturating_mul(permille)
        .div_ceil(1_000)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

/// Times `op` once per sample, storing per-sample nanoseconds.
pub fn time_samples(samples: &mut [u64], mut op: impl FnMut(u64)) {
    for (index, sample) in samples.iter_mut().enumerate() {
        let index = u64::try_from(index).unwrap_or(u64::MAX);
        let started = Instant::now();
        op(index);
        *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    }
}

/// Percentiles and mean for one sample set. Sorts in place. The mean is
/// reported for continuity but is outlier-sensitive; p50-p99.9 are the robust
/// shape, and max records the worst observed scheduler interference.
pub struct SampleStats {
    pub mean: u128,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub max: u64,
}

/// Sorts the sample set and derives percentiles plus the arithmetic mean.
///
/// # Panics
///
/// Panics on an empty sample set or a percentile-ordering violation.
pub fn analyze(samples: &mut [u64]) -> SampleStats {
    assert!(!samples.is_empty(), "sample sets are non-empty");
    samples.sort_unstable();
    let total: u128 = samples.iter().map(|sample| u128::from(*sample)).sum();
    let count = u128::try_from(samples.len()).unwrap_or(u128::MAX);
    let stats = SampleStats {
        mean: total / count,
        p50: percentile(samples, 500),
        p90: percentile(samples, 900),
        p99: percentile(samples, 990),
        p99_9: percentile(samples, 999),
        max: samples[samples.len() - 1],
    };
    assert!(
        stats.max >= stats.p99_9
            && stats.p99_9 >= stats.p99
            && stats.p99 >= stats.p90
            && stats.p90 >= stats.p50,
        "percentile ordering"
    );
    stats
}

fn run_pair(
    gateway: &mut Gateway<2, 8, 2, 2>,
    reports: &mut ReportBuffer<1>,
    first_id: u64,
) -> Result<(), hft_gateway::GatewayError> {
    let sell = encode_new_order(order(first_id, AccountId(1), Side::Sell));
    gateway.process_frame(&RxFrame::from_bytes(&sell), reports)?;
    let buy = encode_new_order(order(first_id + 1, AccountId(2), Side::Buy));
    gateway.process_frame(&RxFrame::from_bytes(&buy), reports)?;
    Ok(())
}

const fn order(id: u64, account_id: AccountId, side: Side) -> NewOrder {
    NewOrder {
        time_in_force: hft_types::TimeInForce::Gtc,
        order_id: OrderId(id),
        account_id,
        instrument_id: InstrumentId(1),
        price: PriceTicks(100),
        quantity: Quantity(1),
        sequence: SequenceNumber(id),
        side,
    }
}

#[cfg(test)]
mod tests {}
