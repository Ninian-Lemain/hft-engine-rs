use hft_journal::{
    DurableSink, FlushPolicy, JournalChannel, JournalError, JournalReader, JournalRecord,
    JournalStatus, JournalStatusReader, JournalWriter, PersistError, PersistenceWorker,
    RECORD_SIZE, RING_CAPACITY,
};
use hft_spsc::SpscQueue;
use std::io;

#[derive(Default)]
struct FaultSink {
    bytes: usize,
    max_write: usize,
    fail_after: Option<usize>,
    flushes: usize,
    fail_flush_on: Option<usize>,
}

impl DurableSink for FaultSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_after.is_some_and(|limit| self.bytes >= limit) {
            return Err(io::ErrorKind::Other.into());
        }
        let count = bytes.len().min(self.max_write.max(1));
        self.bytes += count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        if self.fail_flush_on == Some(self.flushes) {
            return Err(io::ErrorKind::Other.into());
        }
        Ok(())
    }
}

fn initial_status(first_sequence: u64) -> JournalStatus {
    JournalStatus {
        next_written_sequence: first_sequence,
        next_durable_sequence: first_sequence,
        producer_closed: false,
        poisoned: false,
        shutdown_complete: false,
    }
}

#[test]
fn dequeue_does_not_publish_persistence_progress() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, mut reader) = channel.split(73);
    let status = writer.status_reader().expect("controlled status");
    assert_eq!(status.snapshot(), initial_status(73));
    writer.enqueue(b"one").expect("enqueue");
    assert_eq!(status.snapshot(), initial_status(73));
    assert_eq!(reader.read().expect("record").sequence().0, 73);
    assert_eq!(status.snapshot(), initial_status(73));
    let refused =
        PersistenceWorker::<_, 2>::new(reader, FaultSink::default(), FlushPolicy::EveryBatch);
    assert!(matches!(refused, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            poisoned: true,
            ..initial_status(73)
        }
    );
}

#[test]
fn raw_constructors_have_no_status_and_keep_reader_reuse() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut queue = SpscQueue::<JournalRecord, RING_CAPACITY>::try_new().expect("queue");
    let (producer, consumer) = queue.split();
    let mut writer = JournalWriter::from_producer(producer, 5);
    assert!(writer.status_reader().is_none());
    let mut reader = JournalReader::from_consumer(consumer, 5);
    writer.enqueue(b"first").expect("enqueue");
    reader.read().expect("read");
    let worker =
        PersistenceWorker::<_, 1>::new(reader, FaultSink::default(), FlushPolicy::OnShutdown)
            .expect("raw worker accepts consumed reader");
    worker.into_sink().expect("raw sink extraction");
}

#[test]
fn worker_rejects_a_controlled_reader_that_failed_validation() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    {
        let (mut writer, _reader) = channel.split(3);
        writer.enqueue(b"old record").expect("enqueue");
        writer.close();
    }
    let (writer, mut reader) = channel.split(4);
    let status = writer.status_reader().expect("controlled status");
    assert!(matches!(
        reader.read(),
        Err(hft_journal::ReadError::SequenceMismatch { .. })
    ));
    let refused =
        PersistenceWorker::<_, 2>::new(reader, FaultSink::default(), FlushPolicy::EveryBatch);
    assert!(matches!(refused, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    assert!(status.snapshot().poisoned);
    assert_eq!(status.snapshot().next_written_sequence, 4);
}

struct InspectSink<'queue> {
    status: JournalStatusReader<'queue>,
    first_sequence: u64,
    writes: usize,
    flushes: usize,
}

impl DurableSink for InspectSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(self.status.snapshot(), initial_status(self.first_sequence));
        self.writes += 1;
        Ok(bytes.len().min(7))
    }

    fn flush(&mut self) -> io::Result<()> {
        let status = self.status.snapshot();
        assert_eq!(status.next_written_sequence, self.first_sequence + 2);
        assert_eq!(status.next_durable_sequence, self.first_sequence);
        assert!(!status.shutdown_complete);
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn short_writes_publish_only_after_the_entire_batch() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(91);
    let status = writer.status_reader().expect("controlled status");
    writer.enqueue(b"one").expect("enqueue");
    writer.enqueue(b"two").expect("enqueue");
    let sink = InspectSink {
        status,
        first_sequence: 91,
        writes: 0,
        flushes: 0,
    };
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert_eq!(worker.drain_batch().expect("batch"), 2);
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 93,
            next_durable_sequence: 93,
            ..initial_status(91)
        }
    );
    let sink = worker.into_sink().expect("sink");
    assert_eq!(sink.writes, 20);
    assert_eq!(sink.flushes, 1);
    assert!(status.snapshot().poisoned);
}

#[test]
fn partial_batch_failure_keeps_the_previous_watermarks() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(40);
    let status = writer.status_reader().expect("controlled status");
    for _ in 0..4 {
        writer.enqueue(b"record").expect("enqueue");
    }
    let sink = FaultSink {
        max_write: 7,
        fail_after: Some(3 * RECORD_SIZE + 7),
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert_eq!(worker.drain_batch().expect("first batch"), 2);
    assert!(matches!(worker.drain_batch(), Err(PersistError::Io(_))));
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 42,
            next_durable_sequence: 42,
            poisoned: true,
            ..initial_status(40)
        }
    );
    assert!(matches!(worker.into_sink(), Err(PersistError::Poisoned)));
}

#[test]
fn flush_failure_keeps_durable_behind_written() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(40);
    let status = writer.status_reader().expect("controlled status");
    for _ in 0..4 {
        writer.enqueue(b"record").expect("enqueue");
    }
    let sink = FaultSink {
        max_write: RECORD_SIZE,
        fail_flush_on: Some(2),
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, sink, FlushPolicy::EveryBatch).expect("worker");
    assert_eq!(worker.drain_batch().expect("first batch"), 2);
    assert!(matches!(worker.drain_batch(), Err(PersistError::Io(_))));
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 44,
            next_durable_sequence: 42,
            poisoned: true,
            ..initial_status(40)
        }
    );
}

#[test]
fn shutdown_flush_failure_never_publishes_completion() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(9);
    let status = writer.status_reader().expect("controlled status");
    writer.enqueue(b"record").expect("enqueue");
    writer.close();
    let sink = FaultSink {
        fail_flush_on: Some(1),
        ..FaultSink::default()
    };
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, sink, FlushPolicy::OnShutdown).expect("worker");
    assert!(matches!(worker.shutdown(), Err(PersistError::Io(_))));
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 10,
            producer_closed: true,
            poisoned: true,
            ..initial_status(9)
        }
    );
}

#[test]
fn shutdown_requires_close_and_publishes_final_progress_once() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(15);
    let status = writer.status_reader().expect("controlled status");
    writer.enqueue(b"record").expect("enqueue");
    let mut worker =
        PersistenceWorker::<_, 2>::new(reader, FaultSink::default(), FlushPolicy::OnShutdown)
            .expect("worker");
    assert!(matches!(worker.shutdown(), Err(PersistError::ProducerOpen)));
    assert_eq!(status.snapshot(), initial_status(15));
    assert_eq!(worker.drain_batch().expect("batch"), 1);
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 16,
            ..initial_status(15)
        }
    );
    writer.enqueue(b"tail").expect("enqueue");
    writer.close();
    assert!(status.snapshot().producer_closed);
    assert!(!status.snapshot().shutdown_complete);
    worker.shutdown().expect("shutdown");
    worker.shutdown().expect("repeated shutdown");
    assert_eq!(worker.drain_batch().expect("empty"), 0);
    let sink = worker.into_sink().expect("sink");
    assert_eq!(sink.flushes, 1);
    assert_eq!(
        status.snapshot(),
        JournalStatus {
            next_written_sequence: 17,
            next_durable_sequence: 17,
            producer_closed: true,
            shutdown_complete: true,
            ..initial_status(15)
        }
    );
}

#[test]
fn abandoning_reader_or_worker_poisons_controlled_status() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    for mode in 0..4 {
        let mut channel = JournalChannel::try_new().expect("channel");
        let (writer, reader) = channel.split(1);
        let status = writer.status_reader().expect("controlled status");
        match mode {
            0 => drop(reader),
            1 => {
                let invalid = PersistenceWorker::<_, 0>::new(
                    reader,
                    FaultSink::default(),
                    FlushPolicy::OnShutdown,
                );
                assert!(
                    matches!(invalid, Err(error) if error.kind() == io::ErrorKind::InvalidInput)
                );
            }
            2 => {
                let worker = PersistenceWorker::<_, 1>::new(
                    reader,
                    FaultSink::default(),
                    FlushPolicy::OnShutdown,
                )
                .expect("worker");
                drop(worker);
            }
            _ => {
                let worker = PersistenceWorker::<_, 1>::new(
                    reader,
                    FaultSink::default(),
                    FlushPolicy::OnShutdown,
                )
                .expect("worker");
                worker
                    .into_sink()
                    .expect("sink extraction remains supported");
            }
        }
        assert_eq!(
            status.snapshot(),
            JournalStatus {
                poisoned: true,
                ..initial_status(1)
            }
        );
    }
}

#[test]
fn concurrent_status_preserves_watermark_order_and_final_counters() {
    const FIRST: u64 = 500;
    const RECORDS: u64 = 10_000;
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let mut channel = JournalChannel::try_new().expect("channel");
    let (mut writer, reader) = channel.split(FIRST);
    let status = writer.status_reader().expect("controlled status");
    std::thread::scope(|scope| {
        let producer = scope.spawn(move || {
            for _ in 0..RECORDS {
                loop {
                    match writer.enqueue(b"record") {
                        Ok(_) => break,
                        Err(JournalError::Saturated) => std::thread::yield_now(),
                        Err(error) => panic!("unexpected enqueue error: {error:?}"),
                    }
                }
            }
            writer.close();
        });
        let persistence = scope.spawn(move || {
            let sink = FaultSink {
                max_write: RECORD_SIZE,
                ..FaultSink::default()
            };
            let mut worker = PersistenceWorker::<_, 7>::new(reader, sink, FlushPolicy::EveryBatch)
                .expect("worker");
            loop {
                worker.drain_batch().expect("batch");
                match worker.shutdown() {
                    Ok(()) => break,
                    Err(PersistError::ProducerOpen) => std::thread::yield_now(),
                    Err(error) => panic!("unexpected shutdown error: {error:?}"),
                }
            }
        });
        let mut previous = initial_status(FIRST);
        loop {
            let current = status.snapshot();
            assert!(!current.poisoned);
            assert!(current.next_durable_sequence <= current.next_written_sequence);
            assert!(current.next_written_sequence >= previous.next_written_sequence);
            assert!(current.next_durable_sequence >= previous.next_durable_sequence);
            if current.shutdown_complete {
                assert!(current.producer_closed);
                assert_eq!(current.next_written_sequence, FIRST + RECORDS);
                assert_eq!(current.next_durable_sequence, FIRST + RECORDS);
                break;
            }
            previous = current;
            std::thread::yield_now();
        }
        producer.join().expect("producer");
        persistence.join().expect("persistence");
    });
}
