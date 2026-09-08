use hft_journal::{
    DecodeError, DurableSink, FlushPolicy, JournalChannel, JournalError, JournalReader,
    JournalRecord, JournalWriter, PersistError, PersistenceWorker, RECORD_SIZE, RING_CAPACITY,
    ReadError, RecoveryError, open_append, record_checksum, recover,
};
use hft_spsc::SpscQueue;
use hft_types::SequenceNumber;
use std::io::{self, Cursor};

fn payload(id: u64) -> [u8; 46] {
    let mut bytes = [0_u8; 46];
    bytes[..8].copy_from_slice(&id.to_be_bytes());
    bytes
}

fn skip_loom_queue_test() -> bool {
    hft_spsc::IS_LOOM_BUILD
}

#[derive(Default)]
struct FaultSink {
    bytes: Vec<u8>,
    max_write: usize,
    interrupts_remaining: usize,
    fail_after: Option<usize>,
    flushes: usize,
    fail_flush: bool,
}

impl DurableSink for FaultSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.interrupts_remaining != 0 {
            self.interrupts_remaining -= 1;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        if self
            .fail_after
            .is_some_and(|limit| self.bytes.len() >= limit)
        {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        let count = bytes.len().min(self.max_write.max(1));
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn encoding_is_stable_and_rejects_bad_fields() {
    assert_eq!(record_checksum(b"123456789"), 0xe306_9283);
    let record = JournalRecord::new(SequenceNumber(9), b"abc").expect("valid record");
    let bytes = record.encode();
    assert_eq!(bytes.len(), RECORD_SIZE);
    assert_eq!(&bytes[0..4], &[0x4a, 0x52, 1, 0]);
    assert_eq!(&bytes[4..12], &9_u64.to_be_bytes());
    assert_eq!(&bytes[12..14], &3_u16.to_be_bytes());
    assert_eq!(JournalRecord::decode(&bytes), Ok(record));

    let mut bad_version = bytes;
    bad_version[2] = 2;
    assert!(matches!(
        JournalRecord::decode(&bad_version),
        Err(DecodeError::UnsupportedVersion { found: 2 })
    ));
    let mut bad_len = bytes;
    bad_len[12..14].copy_from_slice(&47_u16.to_be_bytes());
    assert!(matches!(
        JournalRecord::decode(&bad_len),
        Err(DecodeError::PayloadLength { found: 47 })
    ));
    let mut corrupt = bytes;
    corrupt[20] ^= 1;
    assert!(matches!(
        JournalRecord::decode(&corrupt),
        Err(DecodeError::ChecksumMismatch { .. })
    ));
}

#[test]
fn shutdown_requires_closed_producer_and_drains_tail() {
    if skip_loom_queue_test() {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    writer.enqueue(b"one").expect("enqueue");
    let sink = FaultSink {
        max_write: RECORD_SIZE,
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 4>::new(reader, sink, FlushPolicy::OnShutdown).expect("worker");
    assert!(matches!(worker.shutdown(), Err(PersistError::ProducerOpen)));
    writer.enqueue(b"two").expect("enqueue after refusal");
    writer.close();
    worker.shutdown().expect("shutdown");
    let sink = worker.into_sink().expect("healthy");
    assert_eq!(sink.flushes, 1);
    assert_eq!(sink.bytes.len(), 2 * RECORD_SIZE);
}

#[test]
fn concurrent_close_and_shutdown_persist_every_record() {
    const RECORDS: usize = 10_000;
    if skip_loom_queue_test() {
        return;
    }
    let record_count = u64::try_from(RECORDS).expect("record count fits");
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    let sink = FaultSink {
        max_write: RECORD_SIZE,
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 32>::new(reader, sink, FlushPolicy::OnShutdown).expect("worker");

    std::thread::scope(|scope| {
        let producer = scope.spawn(move || {
            for sequence in 1..=record_count {
                loop {
                    match writer.enqueue(&payload(sequence)) {
                        Ok(written) => {
                            assert_eq!(written, SequenceNumber(sequence));
                            break;
                        }
                        Err(JournalError::Saturated) => std::thread::yield_now(),
                        Err(error) => panic!("unexpected enqueue error: {error:?}"),
                    }
                }
            }
            writer.close();
        });

        loop {
            worker.drain_batch().expect("concurrent drain");
            match worker.shutdown() {
                Ok(()) => break,
                Err(PersistError::ProducerOpen) => std::thread::yield_now(),
                Err(error) => panic!("unexpected shutdown error: {error:?}"),
            }
        }
        producer.join().expect("producer thread");
    });

    let sink = worker.into_sink().expect("healthy sink");
    assert_eq!(sink.flushes, 1);
    assert_eq!(sink.bytes.len(), RECORDS * RECORD_SIZE);
    let recovered = recover(&mut Cursor::new(sink.bytes), 1).expect("recovery");
    assert_eq!(recovered.records, record_count);
    assert_eq!(recovered.next_sequence, record_count + 1);
}

#[test]
fn oversized_payload_and_sequence_overflow_publish_nothing() {
    if skip_loom_queue_test() {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("queue");
    let (producer, mut consumer) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, u64::MAX);
    assert_eq!(
        writer.enqueue(&[0; 47]),
        Err(JournalError::SequenceOverflow)
    );
    assert_eq!(writer.enqueue(b"ok"), Err(JournalError::SequenceOverflow));
    assert!(consumer.try_pop().is_none());

    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("queue");
    let (producer, mut consumer) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, 1);
    assert_eq!(
        writer.enqueue(&[0; 47]),
        Err(JournalError::PayloadTooLarge { len: 47 })
    );
    assert_eq!(writer.next_sequence(), 1);
    assert!(consumer.try_pop().is_none());
}

#[test]
fn reader_latches_sequence_failure() {
    if skip_loom_queue_test() {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("queue");
    let (mut producer, consumer) = queue.split();
    producer
        .try_push(JournalRecord::new(SequenceNumber(2), b"gap").expect("record"))
        .expect("space");
    producer
        .try_push(JournalRecord::new(SequenceNumber(1), b"later").expect("record"))
        .expect("space");
    let mut reader = JournalReader::from_consumer(consumer, 1);
    assert!(matches!(
        reader.read(),
        Err(ReadError::SequenceMismatch { .. })
    ));
    assert_eq!(reader.read(), Err(ReadError::Poisoned));
    assert!(reader.is_poisoned());
}

#[test]
fn persistence_handles_short_and_interrupted_writes_then_flushes() {
    if skip_loom_queue_test() {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    for id in 1..=3 {
        writer.enqueue(&payload(id)).expect("enqueue");
    }
    writer.close();
    let sink = FaultSink {
        max_write: 7,
        interrupts_remaining: 3,
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert_eq!(worker.drain_batch().expect("first batch"), 2);
    assert_eq!(worker.drain_batch().expect("second batch"), 1);
    worker.shutdown().expect("shutdown");
    let sink = worker.into_sink().expect("healthy sink");
    assert_eq!(sink.bytes.len(), 3 * RECORD_SIZE);
    assert_eq!(sink.flushes, 3);
    let recovered = recover(&mut Cursor::new(sink.bytes), 1).expect("recovery");
    assert_eq!(recovered.records, 3);
    assert_eq!(recovered.next_sequence, 4);
}

enum InvalidWriteSink {
    Zero,
    Oversized,
}

impl DurableSink for InvalidWriteSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Zero => Ok(0),
            Self::Oversized => Ok(bytes.len() + 1),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn invalid_sink_write_counts_poison_persistence() {
    if skip_loom_queue_test() {
        return;
    }
    for (sink, kind) in [
        (InvalidWriteSink::Zero, io::ErrorKind::WriteZero),
        (InvalidWriteSink::Oversized, io::ErrorKind::InvalidData),
    ] {
        let mut channel = JournalChannel::try_new().expect("channel");
        let (mut writer, reader) = channel.split(1);
        writer.enqueue(b"one").expect("enqueue");
        writer.close();
        let mut worker =
            PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
        assert!(matches!(
            worker.drain_batch(),
            Err(PersistError::Io(error)) if error.kind() == kind
        ));
        assert!(matches!(worker.shutdown(), Err(PersistError::Poisoned)));
    }
}

#[test]
fn persistence_failure_poisoning_is_permanent() {
    if skip_loom_queue_test() {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    writer.enqueue(b"one").expect("enqueue");
    writer.close();
    let sink = FaultSink {
        max_write: 8,
        fail_after: Some(8),
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert!(matches!(worker.drain_batch(), Err(PersistError::Io(_))));
    assert!(matches!(worker.drain_batch(), Err(PersistError::Poisoned)));
    assert!(matches!(worker.shutdown(), Err(PersistError::Poisoned)));
    assert!(matches!(worker.into_sink(), Err(PersistError::Poisoned)));
}

#[test]
fn flush_failure_poisoning_is_permanent() {
    if skip_loom_queue_test() {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(1);
    writer.enqueue(b"one").expect("enqueue");
    writer.close();
    let sink = FaultSink {
        max_write: RECORD_SIZE,
        fail_flush: true,
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert!(matches!(worker.drain_batch(), Err(PersistError::Io(_))));
    assert!(matches!(worker.drain_batch(), Err(PersistError::Poisoned)));
}

#[test]
fn recovery_rejects_truncation_corruption_duplicates_and_gaps() {
    let first = JournalRecord::new(SequenceNumber(1), b"one")
        .expect("record")
        .encode();
    let second = JournalRecord::new(SequenceNumber(2), b"two")
        .expect("record")
        .encode();
    let mut valid = Vec::from(first);
    valid.extend_from_slice(&second);
    assert_eq!(
        recover(&mut Cursor::new(&valid), 1)
            .expect("valid")
            .next_sequence,
        3
    );

    assert!(matches!(
        recover(&mut Cursor::new(&valid[..RECORD_SIZE + 11]), 1),
        Err(RecoveryError::Truncated { bytes: 11 })
    ));
    let mut corrupt = valid.clone();
    corrupt[RECORD_SIZE + 30] ^= 1;
    assert!(matches!(
        recover(&mut Cursor::new(corrupt), 1),
        Err(RecoveryError::Decode { .. })
    ));

    let mut duplicate = Vec::from(first);
    duplicate.extend_from_slice(&first);
    assert!(matches!(
        recover(&mut Cursor::new(duplicate), 1),
        Err(RecoveryError::Sequence { .. })
    ));
    let gap = JournalRecord::new(SequenceNumber(3), b"three")
        .expect("record")
        .encode();
    let mut missing = Vec::from(first);
    missing.extend_from_slice(&gap);
    assert!(matches!(
        recover(&mut Cursor::new(missing), 1),
        Err(RecoveryError::Sequence { .. })
    ));
}

#[test]
fn crash_points_recover_an_ordered_prefix_or_fail_closed() {
    let first = JournalRecord::new(SequenceNumber(1), b"one")
        .expect("record")
        .encode();
    let second = JournalRecord::new(SequenceNumber(2), b"two")
        .expect("record")
        .encode();

    for (bytes, records, next_sequence) in [
        (&[][..], 0, 1),
        (&first[..], 1, 2),
        (&[first.as_slice(), second.as_slice()].concat()[..], 2, 3),
    ] {
        let recovered = recover(&mut Cursor::new(bytes), 1).expect("ordered prefix");
        assert_eq!(recovered.records, records);
        assert_eq!(recovered.next_sequence, next_sequence);
    }

    for partial_len in [1, 13, RECORD_SIZE - 1] {
        let mut partial = Vec::from(first);
        partial.extend_from_slice(&second[..partial_len]);
        assert!(matches!(
            recover(&mut Cursor::new(partial), 1),
            Err(RecoveryError::Truncated { bytes }) if bytes == partial_len
        ));
    }
}

#[test]
fn saturation_does_not_consume_sequence() {
    if skip_loom_queue_test() {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("queue");
    let (producer, mut consumer) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, 1);
    for _ in 0..RING_CAPACITY {
        writer.enqueue(b"x").expect("space");
    }
    assert_eq!(writer.enqueue(b"full"), Err(JournalError::Saturated));
    assert_eq!(writer.next_sequence(), RING_CAPACITY as u64 + 1);
    assert!(consumer.try_pop().is_some());
    assert_eq!(
        writer.enqueue(b"resume").expect("space").0,
        RING_CAPACITY as u64 + 1
    );
}

#[test]
fn file_restart_derives_next_sequence() {
    if skip_loom_queue_test() {
        return;
    }
    let mut path = std::env::temp_dir();
    path.push(format!("hft-journal-{}-restart.bin", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let result = (|| -> Result<(), String> {
        let (file, empty) = open_append(&path, 1).map_err(|error| format!("open: {error:?}"))?;
        assert_eq!(empty.next_sequence, 1);
        let mut channel =
            JournalChannel::try_new().map_err(|error| format!("channel: {error:?}"))?;
        let (mut writer, reader) = channel.split(empty.next_sequence);
        writer.enqueue(b"one").map_err(|error| error.to_string())?;
        writer.enqueue(b"two").map_err(|error| error.to_string())?;
        writer.close();
        let mut worker = PersistenceWorker::<_, 8>::new(reader, file, FlushPolicy::EveryBatch)
            .map_err(|error| error.to_string())?;
        worker
            .shutdown()
            .map_err(|error| format!("shutdown: {error:?}"))?;
        drop(worker);

        let (_file, recovered) =
            open_append(&path, 1).map_err(|error| format!("restart: {error:?}"))?;
        assert_eq!(recovered.records, 2);
        assert_eq!(recovered.next_sequence, 3);
        Ok(())
    })();

    std::fs::remove_file(&path).expect("remove fixture");
    result.expect("file restart");
}
