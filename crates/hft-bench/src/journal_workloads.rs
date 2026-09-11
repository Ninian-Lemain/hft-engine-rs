//! Journal benchmark cells for record, handoff, persistence, and recovery work.

use crate::record::{BenchRecord, Extra};
use crate::{ALLOCATIONS, DEALLOCATIONS, analyze};
use hft_journal::{
    DurableSink, FlushPolicy, JournalChannel, JournalReader, JournalRecord, JournalWriter,
    PersistenceWorker, RECORD_SIZE, RING_CAPACITY, recover,
};
use hft_spsc::SpscQueue;
use hft_types::SequenceNumber;
use std::hint::black_box;
use std::io::{self, Cursor};
use std::sync::atomic::Ordering;
use std::time::Instant;

const PAYLOAD: [u8; 46] = [0x4a; 46];
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fill_record(record: &mut BenchRecord, latencies: &mut [u64]) {
    let sample_count = latencies.len();
    let stats = analyze(latencies);
    record.samples = sample_count;
    record.mean_ns = u64::try_from(stats.mean).unwrap_or(u64::MAX);
    record.p50_ns = stats.p50;
    record.p90_ns = stats.p90;
    record.p99_ns = stats.p99;
    record.p99_9_ns = stats.p99_9;
    record.max_ns = stats.max;
    record.ops_per_second = 1_000_000_000_u64
        .checked_div(record.mean_ns.max(1))
        .unwrap_or(0);
}

/// Checksum alternatives and complete record operations.
///
/// # Panics
///
/// Panics if record construction or verification fails.
pub fn journal_checksum_benchmarks(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }

    for sequence in 0..64 {
        black_box(previous_record_checksum(sequence, &PAYLOAD));
        let record =
            JournalRecord::new(SequenceNumber(sequence), &PAYLOAD).expect("fixed payload fits");
        black_box(record.verify());
    }

    let mut latencies = vec![0_u64; samples];
    let before = allocation_counts();
    let mut digest = 0_u64;
    for (index, sample) in latencies.iter_mut().enumerate() {
        let sequence = u64::try_from(index).unwrap_or(u64::MAX);
        let started = Instant::now();
        let checksum = previous_record_checksum(black_box(sequence), black_box(&PAYLOAD));
        *sample = elapsed_ns(started);
        digest ^= checksum.rotate_left(u32::try_from(index % 64).unwrap_or(0));
    }
    push_journal_record(
        out,
        "journal_checksum_fnv_previous",
        "fnv_derived",
        &mut latencies,
        before,
        digest,
    );

    let mut bytes = JournalRecord::new(SequenceNumber(0), &PAYLOAD)
        .expect("fixed payload fits")
        .encode();
    bytes[14..18].fill(0);
    let before = allocation_counts();
    let mut digest = 0_u64;
    for (index, sample) in latencies.iter_mut().enumerate() {
        let sequence = u64::try_from(index).unwrap_or(u64::MAX);
        bytes[4..12].copy_from_slice(&sequence.to_be_bytes());
        let started = Instant::now();
        let checksum = hft_journal::record_checksum(black_box(&bytes));
        *sample = elapsed_ns(started);
        digest ^= u64::from(checksum).rotate_left(u32::try_from(index % 64).unwrap_or(0));
    }
    push_journal_record(
        out,
        "journal_checksum_selected",
        "crc32c_selected",
        &mut latencies,
        before,
        digest,
    );

    let before = allocation_counts();
    let mut digest = 0_u64;
    for (index, sample) in latencies.iter_mut().enumerate() {
        let sequence = SequenceNumber(u64::try_from(index).unwrap_or(u64::MAX));
        let started = Instant::now();
        let record = JournalRecord::new(black_box(sequence), black_box(&PAYLOAD))
            .expect("fixed payload fits");
        *sample = elapsed_ns(started);
        digest ^= record
            .sequence()
            .0
            .rotate_left(u32::try_from(index % 64).unwrap_or(0));
        black_box(record);
    }
    push_journal_record(
        out,
        "journal_record_create",
        "crc32c_selected",
        &mut latencies,
        before,
        digest,
    );

    let record = JournalRecord::new(SequenceNumber(1), &PAYLOAD).expect("fixed payload fits");
    let before = allocation_counts();
    let mut verified = 0_u64;
    for sample in &mut latencies {
        let started = Instant::now();
        let valid = black_box(&record).verify();
        *sample = elapsed_ns(started);
        verified += u64::from(valid);
    }
    assert_eq!(verified, u64::try_from(samples).unwrap_or(u64::MAX));
    push_journal_record(
        out,
        "journal_record_verify",
        "crc32c_selected",
        &mut latencies,
        before,
        verified,
    );
}

fn push_journal_record(
    out: &mut Vec<BenchRecord>,
    scenario: &'static str,
    algorithm: &'static str,
    latencies: &mut [u64],
    before: (u64, u64),
    checksum: u64,
) {
    let after = allocation_counts();
    assert_eq!(after, before, "{scenario} allocation gate");
    let mut record = BenchRecord::new(
        "component",
        "journal",
        scenario,
        &[
            ("algorithm", Extra::Text(algorithm)),
            ("cpu_crc", Extra::Text(cpu_crc_feature())),
        ],
    );
    fill_record(&mut record, latencies);
    record.allocations = after.0.saturating_sub(before.0);
    record.deallocations = after.1.saturating_sub(before.1);
    record.checksum = checksum;
    out.push(record);
}

fn cpu_crc_feature() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        return "sse4.2";
    }
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        return "crc";
    }
    "portable"
}

fn allocation_counts() -> (u64, u64) {
    (
        ALLOCATIONS.load(Ordering::SeqCst),
        DEALLOCATIONS.load(Ordering::SeqCst),
    )
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn previous_record_checksum(sequence: u64, payload: &[u8]) -> u64 {
    let sequence_bytes = sequence.to_be_bytes();
    let length_bytes = u64::try_from(payload.len())
        .unwrap_or(u64::MAX)
        .to_be_bytes();
    let mut lanes = [FNV_OFFSET; 8];

    mix_fnv_columns(&mut lanes, &sequence_bytes);
    for chunk in payload.chunks_exact(8) {
        mix_fnv_columns(&mut lanes, chunk);
    }
    let remainder = &payload[(payload.len() / 8) * 8..];
    for (column, byte) in remainder.iter().enumerate() {
        lanes[column] ^= u64::from(*byte);
        lanes[column] = lanes[column].wrapping_mul(FNV_PRIME);
    }
    mix_fnv_columns(&mut lanes, &length_bytes);

    lanes.into_iter().fold(FNV_OFFSET, |hash, lane| {
        (hash ^ lane).wrapping_mul(FNV_PRIME)
    })
}

fn mix_fnv_columns(lanes: &mut [u64; 8], bytes: &[u8]) {
    for (lane, byte) in lanes.iter_mut().zip(bytes) {
        *lane ^= u64::from(*byte);
        *lane = lane.wrapping_mul(FNV_PRIME);
    }
}

/// Matching-side enqueue: stamp checksum and push into the ring.
///
/// # Panics
///
/// Panics if a measured enqueue fails or an allocation gate trips.
pub fn journal_benchmark(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("ring");
    let (producer, mut consumer) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, 1);

    // Warm-up: fill half the ring untimed, then drain it so the measured
    // window starts with a known-empty ring.
    for _ in 1..=(RING_CAPACITY / 2) as u64 {
        writer.enqueue(&PAYLOAD).expect("warm-up fits");
        let _ = consumer.try_pop();
    }

    let mut latencies = vec![0_u64; samples];
    let allocations_before = ALLOCATIONS.load(Ordering::SeqCst);
    let deallocations_before = DEALLOCATIONS.load(Ordering::SeqCst);
    let mut checksum = 0_u64;
    let mut drained = 0_u64;
    for (offset, sample) in latencies.iter_mut().enumerate() {
        // Steady state: one enqueue timed, one drain untimed so the ring
        // never fills across the run.
        let started = Instant::now();
        let sequence = writer.enqueue(&PAYLOAD).expect("measured enqueue fits");
        *sample = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if consumer.try_pop().is_some() {
            drained += 1;
        }
        checksum_fold(&mut checksum, sequence.0, offset);
    }
    assert_gate(
        (allocations_before, deallocations_before),
        "journal_enqueue",
    );
    drain_ring(&mut consumer);

    let mut record = BenchRecord::new(
        "component",
        "journal",
        "journal_enqueue",
        &[("tif", Extra::Text("journal"))],
    );
    fill_record(&mut record, &mut latencies);
    record.checksum = checksum ^ drained.rotate_left(1);
    out.push(record);
}
/// Persistence-side drain throughput over a pre-filled ring.
///
/// # Panics
///
/// Panics if a drain fails or an allocation gate trips.
pub fn journal_drain_benchmark(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    persistence_batch_benchmark::<1>(samples, out);
    persistence_batch_benchmark::<8>(samples, out);
    persistence_batch_benchmark::<32>(samples, out);
    recovery_scan_benchmark(samples, out);
    saturation_benchmark(samples, out);
}

/// Measures controlled-channel enqueue and persistence with both flush policies.
///
/// # Panics
///
/// Panics on a record, sequence, persistence, or allocation mismatch.
pub fn controlled_journal_benchmarks(samples: usize, out: &mut Vec<BenchRecord>) {
    if samples == 0 {
        return;
    }
    controlled_enqueue(samples, out);
    for policy in [FlushPolicy::OnShutdown, FlushPolicy::EveryBatch] {
        controlled_persistence::<1>(samples, policy, out);
        controlled_persistence::<8>(samples, policy, out);
        controlled_persistence::<32>(samples, policy, out);
    }
}

fn controlled_enqueue(samples: usize, out: &mut Vec<BenchRecord>) {
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, mut reader) = channel.split(1);
    for _ in 0..RING_CAPACITY / 2 {
        writer.enqueue(&PAYLOAD).expect("warm-up enqueue");
        reader.read().expect("warm-up read");
    }
    let mut latencies = vec![0_u64; samples];
    let mut checksum = 0_u64;
    let before = allocation_counts();
    for (offset, sample) in latencies.iter_mut().enumerate() {
        let started = Instant::now();
        let sequence = writer.enqueue(black_box(&PAYLOAD)).expect("enqueue");
        *sample = elapsed_ns(started);
        let record = reader.read().expect("read enqueued record");
        assert_eq!(record.sequence(), sequence);
        assert_eq!(record.slice(), PAYLOAD);
        checksum_fold(&mut checksum, sequence.0, offset);
    }
    assert_gate(before, "controlled_journal_enqueue");
    writer.close();
    let mut record = BenchRecord::new(
        "component",
        "journal",
        "controlled_journal_enqueue",
        &[("capacity", Extra::U64(RING_CAPACITY as u64))],
    );
    fill_record(&mut record, &mut latencies);
    record.checksum = checksum;
    out.push(record);
}

fn controlled_persistence<const BATCH: usize>(
    samples: usize,
    policy: FlushPolicy,
    out: &mut Vec<BenchRecord>,
) {
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    for _ in 0..RING_CAPACITY {
        writer.enqueue(&PAYLOAD).expect("prefill");
    }
    writer.close();
    let mut worker = PersistenceWorker::<_, BATCH>::new(reader, MemorySink::new(), policy)
        .expect("nonzero batch");
    assert_eq!(worker.drain_batch().expect("warm-up drain"), BATCH);
    let occupancy = RING_CAPACITY - BATCH;
    let batches = samples.min(occupancy / BATCH);
    let mut latencies = vec![0_u64; batches];
    let mut drained_total = 0_u64;
    let before = allocation_counts();
    for sample in &mut latencies {
        let started = Instant::now();
        let drained = worker.drain_batch().expect("controlled drain");
        *sample = elapsed_ns(started) / BATCH as u64;
        assert_eq!(drained, BATCH);
        drained_total += drained as u64;
    }
    assert_gate(before, "controlled_journal_persistence_memory");
    worker.shutdown().expect("closed channel shutdown");
    let sink = worker.into_sink().expect("healthy sink");
    assert_eq!(sink.len, RECORD_SIZE * RING_CAPACITY);
    let mut record = BenchRecord::new(
        "component",
        "journal",
        "controlled_journal_persistence_memory",
        &[
            ("batch", Extra::U64(BATCH as u64)),
            ("occupancy", Extra::U64(occupancy as u64)),
            (
                "flush_policy",
                Extra::Text(match policy {
                    FlushPolicy::EveryBatch => "every_batch",
                    FlushPolicy::OnShutdown => "on_shutdown",
                }),
            ),
        ],
    );
    fill_record(&mut record, &mut latencies);
    record.checksum = drained_total;
    out.push(record);
}

struct MemorySink {
    bytes: Box<[u8]>,
    len: usize,
}

impl MemorySink {
    fn new() -> Self {
        Self {
            bytes: vec![0; RECORD_SIZE * RING_CAPACITY].into_boxed_slice(),
            len: 0,
        }
    }
}

impl DurableSink for MemorySink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = self.bytes.len().saturating_sub(self.len);
        let count = remaining.min(bytes.len());
        self.bytes[self.len..self.len + count].copy_from_slice(&bytes[..count]);
        self.len += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn persistence_batch_benchmark<const BATCH: usize>(samples: usize, out: &mut Vec<BenchRecord>) {
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("ring");
    {
        let (producer, _) = queue.split();
        let mut writer = JournalWriter::from_producer(producer, 1);
        for _ in 0..RING_CAPACITY {
            writer.enqueue(&PAYLOAD).expect("prefill fits");
        }
    }

    let reader = JournalReader::from_consumer(split_consumer(&mut queue), 1);
    let sink = MemorySink::new();
    let mut worker = PersistenceWorker::<_, BATCH>::new(reader, sink, FlushPolicy::OnShutdown)
        .expect("nonzero batch");
    worker.drain_batch().expect("warm-up memory drain");
    let measured_occupancy = RING_CAPACITY.saturating_sub(BATCH);
    let batches = samples.min(measured_occupancy / BATCH).max(1);
    let mut latencies = vec![0_u64; batches];
    let mut committed_total = 0_usize;
    let before = allocation_counts();
    for sample in &mut latencies {
        let started = Instant::now();
        let drained = worker.drain_batch().expect("memory drain");
        let elapsed = elapsed_ns(started);
        *sample = elapsed.checked_div(drained.max(1) as u64).unwrap_or(0);
        committed_total += drained;
    }

    let mut record = BenchRecord::new(
        "component",
        "journal",
        "journal_persistence_memory",
        &[
            ("batch", Extra::U64(BATCH as u64)),
            ("occupancy", Extra::U64(measured_occupancy as u64)),
            ("backpressure_events", Extra::U64(0)),
        ],
    );
    fill_record(&mut record, &mut latencies);
    let after = allocation_counts();
    assert_eq!(after, before, "journal persistence allocation gate");
    record.allocations = after.0.saturating_sub(before.0);
    record.deallocations = after.1.saturating_sub(before.1);
    record.checksum = u64::try_from(committed_total).unwrap_or(u64::MAX);
    out.push(record);
}

fn recovery_scan_benchmark(samples: usize, out: &mut Vec<BenchRecord>) {
    const RECORDS: usize = 512;
    let mut bytes = Vec::with_capacity(RECORDS * RECORD_SIZE);
    for sequence in 1..=RECORDS as u64 {
        let encoded = JournalRecord::new(SequenceNumber(sequence), &PAYLOAD)
            .expect("fixed payload fits")
            .encode();
        bytes.extend_from_slice(&encoded);
    }
    recover(&mut Cursor::new(bytes.as_slice()), 1).expect("warm-up recovery image");

    let mut latencies = vec![0_u64; samples];
    let before = allocation_counts();
    let mut digest = 0_u64;
    for sample in &mut latencies {
        let mut input = Cursor::new(bytes.as_slice());
        let started = Instant::now();
        let result = recover(&mut input, 1).expect("valid recovery image");
        *sample = elapsed_ns(started) / RECORDS as u64;
        digest ^= result.next_sequence;
    }
    push_journal_record(
        out,
        "journal_recovery_scan",
        "crc32c_selected",
        &mut latencies,
        before,
        digest,
    );
}

fn saturation_benchmark(samples: usize, out: &mut Vec<BenchRecord>) {
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("ring");
    let (producer, _) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, 1);
    for _ in 0..RING_CAPACITY {
        writer.enqueue(&PAYLOAD).expect("prefill fits");
    }
    assert!(writer.enqueue(&PAYLOAD).is_err());

    let mut latencies = vec![0_u64; samples];
    let before = allocation_counts();
    let mut backpressure = 0_u64;
    for sample in &mut latencies {
        let started = Instant::now();
        let result = writer.enqueue(black_box(&PAYLOAD));
        *sample = elapsed_ns(started);
        backpressure += u64::from(result.is_err());
    }
    let after = allocation_counts();
    assert_eq!(after, before, "journal saturation allocation gate");
    let mut record = BenchRecord::new(
        "component",
        "journal",
        "journal_saturation_refusal",
        &[
            ("capacity", Extra::U64(RING_CAPACITY as u64)),
            ("occupancy", Extra::U64(RING_CAPACITY as u64)),
            ("backpressure_events", Extra::U64(backpressure)),
        ],
    );
    fill_record(&mut record, &mut latencies);
    record.allocations = after.0.saturating_sub(before.0);
    record.deallocations = after.1.saturating_sub(before.1);
    record.checksum = backpressure;
    out.push(record);
}

/// Saturation: fill to capacity, assert explicit refusal, drain, continue.
#[test]
fn saturation_smoke() {
    // Under a Loom build the ring's primitives panic outside a model; the
    // Loom unit tests in hft-spsc cover this path there.
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().unwrap();
    let (producer, mut consumer) = queue.split();
    let mut writer = hft_journal::JournalWriter::from_producer(producer, 1);
    for _ in 1..=RING_CAPACITY as u64 {
        writer.enqueue(&[0_u8; 46]).unwrap();
    }
    assert!(matches!(
        writer.enqueue(&[1_u8; 46]),
        Err(hft_journal::JournalError::Saturated)
    ));
    while consumer.try_pop().is_some() {}
    writer.enqueue(&[2_u8; 46]).unwrap();
}

fn checksum_fold(checksum: &mut u64, sequence: u64, offset: usize) {
    *checksum ^= sequence.wrapping_shr(u32::try_from(offset % 64).unwrap_or(0));
}

fn drain_ring(consumer: &mut hft_spsc::Consumer<'_, JournalRecord, RING_CAPACITY>) {
    while consumer.try_pop().is_some() {}
}

fn assert_gate(before: (u64, u64), context: &'static str) {
    assert_eq!(
        ALLOCATIONS.load(Ordering::SeqCst),
        before.0,
        "{context} allocations"
    );
    assert_eq!(
        DEALLOCATIONS.load(Ordering::SeqCst),
        before.1,
        "{context} deallocations"
    );
}

fn split_consumer(
    queue: &mut SpscQueue<JournalRecord, RING_CAPACITY>,
) -> hft_spsc::Consumer<'_, JournalRecord, RING_CAPACITY> {
    let (_, consumer) = queue.split();
    consumer
}
