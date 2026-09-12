use crate::record::{BenchRecord, Extra};
use crate::{allocation_gate, assert_allocation_gate, push_latency_record};
use hft_engine::{EngineBuilder, EngineParts, EngineStorage};
use hft_events::{BoundedEventEngine, EventBatch};
use hft_gateway::Gateway;
use hft_journal::{DurableSink, FlushPolicy, JournalChannel, PersistenceWorker};
use hft_recovery::encode_snapshot;
use hft_risk::{RiskEngine, RiskLimits};
use hft_spsc::SpscQueue;
use hft_types::{
    AccountId, CancelOrder, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity,
    SequenceNumber, Side, TimeInForce,
};
use hft_wire::{encode_cancel_order, encode_new_order};
use std::{hint::black_box, io, time::Instant};

const WARMUP: usize = 128;
const INSTRUMENT: InstrumentId = InstrumentId(7);
type Builder = EngineBuilder<1, 8, 2, 4, 2>;
type Events<'queue> = BoundedEventEngine<'queue, 1, 8, 2, 4, 2, 4, 2>;

#[derive(Default)]
struct Sink;

impl DurableSink for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        black_box(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn limits() -> RiskLimits {
    RiskLimits {
        max_quantity: Quantity(10),
        max_notional: 10_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 8,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    }
}

fn command(step: usize) -> Command {
    let sequence = u64::try_from(step).expect("step") + 1;
    if step % 2 == 0 {
        Command::NewOrder(NewOrder {
            order_id: OrderId(sequence),
            account_id: AccountId(1),
            instrument_id: INSTRUMENT,
            sequence: SequenceNumber(sequence),
            price: PriceTicks(100),
            quantity: Quantity(2),
            side: Side::Buy,
            time_in_force: TimeInForce::Gtc,
        })
    } else {
        Command::CancelOrder(CancelOrder {
            order_id: OrderId(sequence - 1),
            account_id: AccountId(1),
            instrument_id: INSTRUMENT,
            sequence: SequenceNumber(sequence),
        })
    }
}

/// # Panics
///
/// Panics on allocation, command failure, or differing final snapshots.
pub fn engine_benchmarks(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    let raw = composition(samples, out);
    let facade = facade(samples, out);
    assert_eq!(raw, facade, "facade state differs from composition");
}

fn facade(samples: usize, out: &mut Vec<BenchRecord>) -> [u8; 32] {
    let mut storage = EngineStorage::<4, 2>::try_new().expect("storage");
    let storage_bytes = size_of_val(&storage);
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = Builder::new(INSTRUMENT, &[(AccountId(1), limits())])
        .expect("builder")
        .build(&mut storage)
        .expect("engine");
    let state_bytes = storage_bytes + size_of_val(&engine);
    let mut worker =
        PersistenceWorker::<_, 1>::new(journal, Sink, FlushPolicy::OnShutdown).expect("worker");
    let mut timings = vec![0; samples];
    let mut checksum = 0_u64;
    let gate = allocation_gate();
    for step in 0..WARMUP + samples {
        let command = command(step);
        let started = Instant::now();
        let result = black_box(&mut engine).process_command(black_box(command));
        black_box(&result);
        let elapsed = started.elapsed().as_nanos();
        result.expect("admission");
        let batch = events.try_pop().expect("batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(worker.drain_batch().expect("drain"), 1);
        if step >= WARMUP {
            timings[step - WARMUP] = u64::try_from(elapsed).unwrap_or(u64::MAX);
            checksum = checksum.wrapping_add(command.sequence().0);
        }
    }
    assert_allocation_gate(gate, "facade admission");
    engine.stop_admission();
    worker.shutdown().expect("shutdown");
    let digest = engine.snapshot().expect("snapshot").digest();
    finish(out, "facade", state_bytes, checksum, &mut timings);
    digest
}

fn composition(samples: usize, out: &mut Vec<BenchRecord>) -> [u8; 32] {
    let mut risk = RiskEngine::new();
    risk.register_account(AccountId(1), limits())
        .expect("account");
    let mut queue = SpscQueue::<EventBatch<4>, 2>::try_new().expect("queue");
    let (producer, mut consumer) = queue.split();
    let mut engine = Events::try_new(Gateway::new(risk, INSTRUMENT), producer).expect("events");
    let mut journal = JournalChannel::try_new().expect("journal");
    let storage_bytes = size_of::<SpscQueue<EventBatch<4>, 2>>() + size_of_val(&journal);
    let (mut writer, reader) = journal.split(1);
    let state_bytes = storage_bytes + size_of_val(&engine) + size_of_val(&writer);
    let mut worker =
        PersistenceWorker::<_, 1>::new(reader, Sink, FlushPolicy::OnShutdown).expect("worker");
    let mut timings = vec![0; samples];
    let mut checksum = 0_u64;
    let gate = allocation_gate();
    for step in 0..WARMUP + samples {
        let command = command(step);
        let started = Instant::now();
        let admitted = black_box(&mut engine)
            .admit(black_box(command))
            .expect("admission");
        let enqueued = match command {
            Command::NewOrder(order) => writer.enqueue(&encode_new_order(order)),
            Command::CancelOrder(cancel) => writer.enqueue(&encode_cancel_order(cancel)),
            Command::ReplaceOrder(_) => unreachable!("fixture contains no replaces"),
        };
        enqueued.expect("journal");
        let result = admitted.apply();
        black_box(&result);
        let elapsed = started.elapsed().as_nanos();
        result.expect("apply");
        let batch = consumer.try_pop().expect("batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(worker.drain_batch().expect("drain"), 1);
        if step >= WARMUP {
            timings[step - WARMUP] = u64::try_from(elapsed).unwrap_or(u64::MAX);
            checksum = checksum.wrapping_add(command.sequence().0);
        }
    }
    assert_allocation_gate(gate, "composed admission");
    writer.close();
    worker.shutdown().expect("shutdown");
    let digest = encode_snapshot(engine.gateway(), engine.gateway().expected_sequence().0 - 1)
        .expect("snapshot")
        .digest();
    finish(out, "composition", state_bytes, checksum, &mut timings);
    digest
}

fn finish(
    out: &mut Vec<BenchRecord>,
    path: &'static str,
    state_bytes: usize,
    checksum: u64,
    timings: &mut [u64],
) {
    push_latency_record(
        out,
        BenchRecord {
            checksum,
            ..BenchRecord::new(
                "gateway",
                "engine",
                "journaled_command",
                &[
                    ("path", Extra::Text(path)),
                    ("state_bytes", Extra::U64(state_bytes as u64)),
                ],
            )
        },
        timings,
    );
}
