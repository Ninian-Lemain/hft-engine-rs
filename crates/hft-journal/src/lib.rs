//! Bounded handoff and durable storage for accepted commands.
#![forbid(unsafe_code)]

use core::fmt;
use hft_spsc::{Consumer, Producer, QueueConfigError, SpscQueue};
use hft_types::SequenceNumber;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const MAX_PAYLOAD: usize = 46;
pub const RECORD_SIZE: usize = 64;
pub const RING_CAPACITY: usize = 1024;
pub const FORMAT_VERSION: u8 = 1;
const MAGIC: u16 = 0x4a52;
const CHECKSUM_OFFSET: usize = 14;
const PAYLOAD_OFFSET: usize = 18;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalError {
    Saturated,
    SequenceOverflow,
    PayloadTooLarge { len: usize },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Saturated => f.write_str("journal ring saturated"),
            Self::SequenceOverflow => f.write_str("journal sequence exhausted"),
            Self::PayloadTooLarge { len } => {
                write!(f, "journal payload length {len} exceeds {MAX_PAYLOAD}")
            }
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalRecord {
    sequence: SequenceNumber,
    checksum: u32,
    len: u16,
    magic: u16,
    payload: [u8; MAX_PAYLOAD],
    version: u8,
    flags: u8,
}

const _: () = assert!(core::mem::size_of::<JournalRecord>() == RECORD_SIZE);
const _: () = assert!(core::mem::align_of::<JournalRecord>() == 8);

impl JournalRecord {
    /// Builds one record without truncating its payload.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::PayloadTooLarge`] when the payload does not fit.
    pub fn new(sequence: SequenceNumber, payload: &[u8]) -> Result<Self, JournalError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(JournalError::PayloadTooLarge { len: payload.len() });
        }
        let mut stored = [0_u8; MAX_PAYLOAD];
        stored[..payload.len()].copy_from_slice(payload);
        let mut record = Self {
            sequence,
            payload: stored,
            checksum: 0,
            len: u16::try_from(payload.len())
                .map_err(|_| JournalError::PayloadTooLarge { len: payload.len() })?,
            magic: MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
        };
        record.checksum = record_checksum(&record.bytes_without_checksum());
        Ok(record)
    }

    #[must_use]
    pub const fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    #[must_use]
    pub fn slice(&self) -> Option<&[u8]> {
        self.payload.get(..usize::from(self.len))
    }

    #[must_use]
    pub fn verify(&self) -> bool {
        self.magic == MAGIC
            && self.version == FORMAT_VERSION
            && self.flags == 0
            && self.slice().is_some()
            && self.checksum == record_checksum(&self.bytes_without_checksum())
    }

    #[must_use]
    pub fn encode(&self) -> [u8; RECORD_SIZE] {
        let mut bytes = self.bytes_without_checksum();
        bytes[CHECKSUM_OFFSET..PAYLOAD_OFFSET].copy_from_slice(&self.checksum.to_be_bytes());
        bytes
    }

    /// Decodes and verifies one complete disk record.
    ///
    /// # Errors
    ///
    /// Returns a format or checksum error for any invalid field.
    pub fn decode(bytes: &[u8; RECORD_SIZE]) -> Result<Self, DecodeError> {
        let magic = u16::from_be_bytes([bytes[0], bytes[1]]);
        if magic != MAGIC {
            return Err(DecodeError::BadMagic { found: magic });
        }
        let version = bytes[2];
        if version != FORMAT_VERSION {
            return Err(DecodeError::UnsupportedVersion { found: version });
        }
        let flags = bytes[3];
        if flags != 0 {
            return Err(DecodeError::UnsupportedFlags { found: flags });
        }
        let sequence = SequenceNumber(u64::from_be_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| DecodeError::Malformed)?,
        ));
        let len = u16::from_be_bytes([bytes[12], bytes[13]]);
        if usize::from(len) > MAX_PAYLOAD {
            return Err(DecodeError::PayloadLength { found: len });
        }
        let checksum = u32::from_be_bytes(
            bytes[CHECKSUM_OFFSET..PAYLOAD_OFFSET]
                .try_into()
                .map_err(|_| DecodeError::Malformed)?,
        );
        let mut payload = [0_u8; MAX_PAYLOAD];
        payload.copy_from_slice(&bytes[PAYLOAD_OFFSET..]);
        let record = Self {
            sequence,
            checksum,
            len,
            magic,
            payload,
            version,
            flags,
        };
        if !record.verify() {
            return Err(DecodeError::ChecksumMismatch { sequence });
        }
        Ok(record)
    }

    fn bytes_without_checksum(&self) -> [u8; RECORD_SIZE] {
        let mut bytes = [0_u8; RECORD_SIZE];
        bytes[0..2].copy_from_slice(&self.magic.to_be_bytes());
        bytes[2] = self.version;
        bytes[3] = self.flags;
        bytes[4..12].copy_from_slice(&self.sequence.0.to_be_bytes());
        bytes[12..14].copy_from_slice(&self.len.to_be_bytes());
        bytes[PAYLOAD_OFFSET..].copy_from_slice(&self.payload);
        bytes
    }
}

/// CRC32C boundary for record creation and verification.
#[must_use]
pub fn record_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    Malformed,
    BadMagic { found: u16 },
    UnsupportedVersion { found: u8 },
    UnsupportedFlags { found: u8 },
    PayloadLength { found: u16 },
    ChecksumMismatch { sequence: SequenceNumber },
}

/// Watermarks start at the channel's first sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalStatus {
    /// First sequence not covered by a fully written batch.
    pub next_written_sequence: u64,
    /// First sequence not covered by a successful flush.
    pub next_durable_sequence: u64,
    pub producer_closed: bool,
    pub poisoned: bool,
    pub shutdown_complete: bool,
}

#[derive(Debug)]
struct StatusState {
    next_written_sequence: AtomicU64,
    next_durable_sequence: AtomicU64,
    producer_closed: AtomicBool,
    poisoned: AtomicBool,
    shutdown_complete: AtomicBool,
}

impl StatusState {
    fn new(first_sequence: u64) -> Self {
        Self {
            next_written_sequence: AtomicU64::new(first_sequence),
            next_durable_sequence: AtomicU64::new(first_sequence),
            producer_closed: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
        }
    }
}

/// Read-only persistence progress for a controlled journal channel.
#[derive(Clone, Copy, Debug)]
pub struct JournalStatusReader<'queue> {
    state: &'queue StatusState,
}

impl JournalStatusReader<'_> {
    /// Reads only the terminal persistence failure flag.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.state.poisoned.load(Ordering::Acquire)
    }

    /// Counters may advance during this call. Durable never exceeds written.
    #[must_use]
    pub fn snapshot(&self) -> JournalStatus {
        // Completion publishes the final counters and producer closure. Read
        // durable before written to include the batch that its flush covered.
        let shutdown_complete = self.state.shutdown_complete.load(Ordering::Acquire);
        let next_durable_sequence = self.state.next_durable_sequence.load(Ordering::Acquire);
        let next_written_sequence = self.state.next_written_sequence.load(Ordering::Acquire);
        JournalStatus {
            next_written_sequence,
            next_durable_sequence,
            producer_closed: self.state.producer_closed.load(Ordering::Acquire),
            poisoned: self.state.poisoned.load(Ordering::Acquire),
            shutdown_complete,
        }
    }
}

struct StatusGuard<'queue> {
    state: Option<&'queue StatusState>,
    shutdown_complete: bool,
}

impl StatusGuard<'_> {
    fn poison(&self) {
        if let Some(state) = self.state {
            state.poisoned.store(true, Ordering::Release);
        }
    }

    fn publish_written(&self, next_sequence: u64) {
        if let Some(state) = self.state {
            state
                .next_written_sequence
                .store(next_sequence, Ordering::Release);
        }
    }

    fn publish_durable(&self, next_sequence: u64) {
        if let Some(state) = self.state {
            // The written watermark precedes this successful flush publication.
            state
                .next_durable_sequence
                .store(next_sequence, Ordering::Release);
        }
    }

    fn complete(&mut self) {
        if let Some(state) = self.state {
            state.shutdown_complete.store(true, Ordering::Release);
        }
        self.shutdown_complete = true;
    }
}

impl Drop for StatusGuard<'_> {
    fn drop(&mut self) {
        if !self.shutdown_complete {
            self.poison();
        }
    }
}

/// Journal queue with producer closure and persistence progress.
pub struct JournalChannel {
    queue: SpscQueue<JournalRecord, RING_CAPACITY>,
    status: StatusState,
}

impl JournalChannel {
    /// # Errors
    ///
    /// Returns the queue configuration error if the fixed capacity is invalid.
    pub fn try_new() -> Result<Self, QueueConfigError> {
        Ok(Self {
            queue: SpscQueue::try_new()?,
            status: StatusState::new(0),
        })
    }

    /// Creates the single writer and reader for one sequence domain.
    pub fn split(&mut self, first_sequence: u64) -> (JournalWriter<'_>, JournalReader<'_>) {
        let Self { queue, status } = self;
        *status = StatusState::new(first_sequence);
        let (producer, consumer) = queue.split();
        (
            JournalWriter::controlled(producer, first_sequence, status),
            JournalReader::controlled(consumer, first_sequence, status),
        )
    }
}

pub struct JournalWriter<'queue> {
    producer: Producer<'queue, JournalRecord, RING_CAPACITY>,
    next_sequence: u64,
    status: Option<&'queue StatusState>,
}

impl<'queue> JournalWriter<'queue> {
    #[must_use]
    pub fn from_producer(
        producer: Producer<'queue, JournalRecord, RING_CAPACITY>,
        first_sequence: u64,
    ) -> Self {
        Self {
            producer,
            next_sequence: first_sequence,
            status: None,
        }
    }

    fn controlled(
        producer: Producer<'queue, JournalRecord, RING_CAPACITY>,
        first_sequence: u64,
        status: &'queue StatusState,
    ) -> Self {
        Self {
            producer,
            next_sequence: first_sequence,
            status: Some(status),
        }
    }

    #[must_use]
    pub fn new(
        queue: &'queue mut SpscQueue<JournalRecord, RING_CAPACITY>,
        first_sequence: u64,
    ) -> Self {
        let (producer, _) = queue.split();
        Self::from_producer(producer, first_sequence)
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Raw SPSC constructors have no persistence status.
    #[must_use]
    pub fn status_reader(&self) -> Option<JournalStatusReader<'queue>> {
        self.status.map(|state| JournalStatusReader { state })
    }

    /// Closes admission for a controlled journal channel.
    ///
    /// Raw SPSC constructors do not carry a close signal.
    pub fn close(self) {
        if let Some(status) = self.status {
            // Release follows every enqueue. Close consumes the only writer,
            // so an acquiring worker cannot miss a later publication.
            status.producer_closed.store(true, Ordering::Release);
        }
    }

    /// Builds and publishes one record without storage access or allocation.
    ///
    /// # Errors
    ///
    /// Returns an explicit length, capacity, or sequence error before changing
    /// the producer sequence.
    pub fn enqueue(&mut self, payload: &[u8]) -> Result<SequenceNumber, JournalError> {
        let next = self
            .next_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceOverflow)?;
        let sequence = SequenceNumber(self.next_sequence);
        let record = JournalRecord::new(sequence, payload)?;
        self.producer
            .try_push(record)
            .map_err(|_| JournalError::Saturated)?;
        self.next_sequence = next;
        Ok(sequence)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedRecord(JournalRecord);

impl ParsedRecord {
    #[must_use]
    pub const fn sequence(&self) -> SequenceNumber {
        self.0.sequence
    }
    #[must_use]
    pub fn slice(&self) -> &[u8] {
        self.0.slice().unwrap_or(&[])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadError {
    Empty,
    ChecksumMismatch {
        sequence: SequenceNumber,
    },
    SequenceMismatch {
        expected: SequenceNumber,
        received: SequenceNumber,
    },
    Poisoned,
}

pub struct JournalReader<'queue> {
    consumer: Consumer<'queue, JournalRecord, RING_CAPACITY>,
    expected_sequence: u64,
    poisoned: bool,
    status: StatusGuard<'queue>,
}

impl<'queue> JournalReader<'queue> {
    #[must_use]
    pub fn from_consumer(
        consumer: Consumer<'queue, JournalRecord, RING_CAPACITY>,
        first_expected: u64,
    ) -> Self {
        Self {
            consumer,
            expected_sequence: first_expected,
            poisoned: false,
            status: StatusGuard {
                state: None,
                shutdown_complete: false,
            },
        }
    }

    fn controlled(
        consumer: Consumer<'queue, JournalRecord, RING_CAPACITY>,
        first_expected: u64,
        status: &'queue StatusState,
    ) -> Self {
        Self {
            consumer,
            expected_sequence: first_expected,
            poisoned: false,
            status: StatusGuard {
                state: Some(status),
                shutdown_complete: false,
            },
        }
    }
    #[must_use]
    pub fn new(
        queue: &'queue mut SpscQueue<JournalRecord, RING_CAPACITY>,
        first_expected: u64,
    ) -> Self {
        let (_, consumer) = queue.split();
        Self::from_consumer(consumer, first_expected)
    }
    /// Reads one ordered record. Validation failures poison the reader.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Empty`] if no record is ready. Invalid records
    /// return a terminal error and all later reads return `Poisoned`.
    pub fn read(&mut self) -> Result<ParsedRecord, ReadError> {
        if self.poisoned {
            return Err(ReadError::Poisoned);
        }
        let record = self.consumer.try_pop().ok_or(ReadError::Empty)?;
        if !record.verify() {
            self.poisoned = true;
            self.status.poison();
            return Err(ReadError::ChecksumMismatch {
                sequence: record.sequence,
            });
        }
        if record.sequence.0 != self.expected_sequence {
            self.poisoned = true;
            self.status.poison();
            return Err(ReadError::SequenceMismatch {
                expected: SequenceNumber(self.expected_sequence),
                received: record.sequence,
            });
        }
        self.expected_sequence = self.expected_sequence.checked_add(1).ok_or_else(|| {
            self.poisoned = true;
            self.status.poison();
            ReadError::Poisoned
        })?;
        Ok(ParsedRecord(record))
    }
    #[must_use]
    pub const fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn producer_is_closed(&self) -> bool {
        self.status
            .state
            .is_some_and(|state| state.producer_closed.load(Ordering::Acquire))
    }
}

pub trait DurableSink {
    /// Writes some bytes to the sink.
    ///
    /// # Errors
    ///
    /// Returns the underlying storage error.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;
    /// Makes all prior writes durable according to the sink contract.
    ///
    /// # Errors
    ///
    /// Returns the underlying storage error.
    fn flush(&mut self) -> io::Result<()>;
}

impl DurableSink for File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Write::write(self, bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Write::flush(self)?;
        self.sync_data()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlushPolicy {
    EveryBatch,
    OnShutdown,
}

#[derive(Debug)]
pub enum PersistError {
    Read(ReadError),
    Io(io::Error),
    ProducerOpen,
    Poisoned,
}

pub struct PersistenceWorker<'queue, S, const BATCH: usize> {
    reader: JournalReader<'queue>,
    sink: S,
    policy: FlushPolicy,
    poisoned: bool,
}

impl<'queue, S: DurableSink, const BATCH: usize> PersistenceWorker<'queue, S, BATCH> {
    /// Creates a persistence worker with a fixed stack batch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when `BATCH` is zero or a controlled reader has
    /// consumed or rejected records. Refusal poisons a controlled channel.
    pub fn new(reader: JournalReader<'queue>, sink: S, policy: FlushPolicy) -> io::Result<Self> {
        if BATCH == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal batch must be nonzero",
            ));
        }
        if reader.status.state.is_some_and(|state| {
            reader.poisoned
                || reader.expected_sequence != state.next_written_sequence.load(Ordering::Acquire)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal reader consumed records before persistence",
            ));
        }
        Ok(Self {
            reader,
            sink,
            policy,
            poisoned: false,
        })
    }
    /// Persists at most `BATCH` records.
    ///
    /// # Errors
    ///
    /// Returns the first validation or storage error and poisons the worker.
    pub fn drain_batch(&mut self) -> Result<usize, PersistError> {
        if self.poisoned {
            return Err(PersistError::Poisoned);
        }
        let mut batch = [[0_u8; RECORD_SIZE]; BATCH];
        let mut count = 0;
        while count < BATCH {
            match self.reader.read() {
                Ok(record) => {
                    batch[count] = record.0.encode();
                    count += 1;
                }
                Err(ReadError::Empty) => break,
                Err(error) => return self.fail(PersistError::Read(error)),
            }
        }
        for record in &batch[..count] {
            if let Err(error) = write_all(&mut self.sink, record) {
                return self.fail(PersistError::Io(error));
            }
        }
        if count != 0 {
            self.reader
                .status
                .publish_written(self.reader.expected_sequence);
        }
        if count != 0 && self.policy == FlushPolicy::EveryBatch {
            if let Err(error) = self.sink.flush() {
                return self.fail(PersistError::Io(error));
            }
            self.reader
                .status
                .publish_durable(self.reader.expected_sequence);
        }
        Ok(count)
    }
    /// Drains the queue and flushes the sink after producer closure.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::ProducerOpen`] until the controlled writer is
    /// closed. Returns the first validation or storage error after closure.
    pub fn shutdown(&mut self) -> Result<(), PersistError> {
        if self.poisoned {
            return Err(PersistError::Poisoned);
        }
        if self.reader.status.shutdown_complete {
            return Ok(());
        }
        if !self.reader.producer_is_closed() {
            return Err(PersistError::ProducerOpen);
        }
        loop {
            if self.drain_batch()? == 0 {
                break;
            }
        }
        if let Err(error) = self.sink.flush() {
            return self.fail(PersistError::Io(error));
        }
        self.reader
            .status
            .publish_durable(self.reader.expected_sequence);
        self.reader.status.complete();
        Ok(())
    }
    /// Returns the sink only when no terminal error occurred.
    /// Taking a controlled sink before shutdown marks its status poisoned.
    ///
    /// # Errors
    ///
    /// Returns `Poisoned` after any persistence failure.
    pub fn into_sink(self) -> Result<S, PersistError> {
        if self.poisoned {
            Err(PersistError::Poisoned)
        } else {
            Ok(self.sink)
        }
    }
    fn fail<T>(&mut self, error: PersistError) -> Result<T, PersistError> {
        self.poisoned = true;
        self.reader.status.poison();
        Err(error)
    }
}

fn write_all(sink: &mut impl DurableSink, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match sink.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "journal write returned zero",
                ));
            }
            Ok(written) if written <= bytes.len() => bytes = &bytes[written..],
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal sink returned an invalid write count",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum RecoveryError {
    Io(io::Error),
    Truncated {
        bytes: usize,
    },
    Decode {
        offset: u64,
        source: DecodeError,
    },
    Sequence {
        expected: SequenceNumber,
        received: SequenceNumber,
    },
    SequenceOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Recovery {
    pub records: u64,
    pub next_sequence: u64,
}

/// Scans a journal without accepting a corrupt or partial tail.
///
/// # Errors
///
/// Returns the first I/O, format, checksum, truncation, or sequence error.
pub fn recover<R: Read>(reader: &mut R, first_sequence: u64) -> Result<Recovery, RecoveryError> {
    let mut bytes = [0_u8; RECORD_SIZE];
    let mut expected = first_sequence;
    let mut records = 0_u64;
    loop {
        let mut filled = 0;
        while filled < RECORD_SIZE {
            match reader.read(&mut bytes[filled..]) {
                Ok(0) if filled == 0 => {
                    return Ok(Recovery {
                        records,
                        next_sequence: expected,
                    });
                }
                Ok(0) => return Err(RecoveryError::Truncated { bytes: filled }),
                Ok(read) => filled += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(RecoveryError::Io(error)),
            }
        }
        let offset = records.saturating_mul(RECORD_SIZE as u64);
        let record = JournalRecord::decode(&bytes)
            .map_err(|source| RecoveryError::Decode { offset, source })?;
        if record.sequence.0 != expected {
            return Err(RecoveryError::Sequence {
                expected: SequenceNumber(expected),
                received: record.sequence,
            });
        }
        expected = expected
            .checked_add(1)
            .ok_or(RecoveryError::SequenceOverflow)?;
        records = records
            .checked_add(1)
            .ok_or(RecoveryError::SequenceOverflow)?;
    }
}

/// Opens a file for append after a successful complete recovery scan.
///
/// # Errors
///
/// Returns the first open, scan, or seek error.
pub fn open_append(path: &Path, first_sequence: u64) -> Result<(File, Recovery), RecoveryError> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
        .map_err(RecoveryError::Io)?;
    file.seek(SeekFrom::Start(0)).map_err(RecoveryError::Io)?;
    let recovery = recover(&mut file, first_sequence)?;
    file.seek(SeekFrom::End(0)).map_err(RecoveryError::Io)?;
    Ok((file, recovery))
}
