use hft_journal::{
    DurableSink, FlushPolicy, JournalChannel, JournalError, JournalRecord, PersistError,
    PersistenceWorker, RECORD_SIZE, RING_CAPACITY, RecoveryError, recover,
};
use hft_types::SequenceNumber;
use std::fmt;
use std::io::{self, Cursor};

const RETAINED_RECORDS: usize = RING_CAPACITY + 1;
const RETAINED_BYTES: usize = RETAINED_RECORDS * RECORD_SIZE;
const SHORT_WRITE_LIMIT: usize = 7;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalFaultResult {
    pub saturation_refusals: u64,
    pub retry_successes: u64,
    pub short_write_calls: u64,
    pub producer_open_refusals: u64,
    pub final_flushes: u64,
    pub recovered_records: u64,
    pub valid_crash_prefixes: u64,
    pub rejected_truncations: u64,
    pub hard_write_failures: u64,
    pub hard_flush_failures: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFaultCheck {
    Channel,
    Fill,
    Saturation,
    SequenceAfterSaturation,
    Worker,
    Drain,
    Retry,
    ProducerOpen,
    Shutdown,
    Sink,
    Recovery,
    CrashPrefix,
    Truncation,
    WriteFailure,
    FlushFailure,
    Poison,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalFaultError {
    check: JournalFaultCheck,
}

impl JournalFaultError {
    const fn at(check: JournalFaultCheck) -> Self {
        Self { check }
    }
}

impl fmt::Display for JournalFaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "journal fault check failed at {:?}", self.check)
    }
}

impl std::error::Error for JournalFaultError {}

struct BoundedSink<const N: usize> {
    bytes: [u8; N],
    len: usize,
    max_write: usize,
    write_calls: u64,
    flushes: u64,
    fail_write_call: Option<u64>,
    fail_flush: bool,
}

impl<const N: usize> BoundedSink<N> {
    const fn new(max_write: usize) -> Self {
        Self {
            bytes: [0; N],
            len: 0,
            max_write,
            write_calls: 0,
            flushes: 0,
            fail_write_call: None,
            fail_flush: false,
        }
    }

    fn slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl<const N: usize> DurableSink for BoundedSink<N> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_calls = self.write_calls.saturating_add(1);
        if self.fail_write_call == Some(self.write_calls) {
            return Err(io::Error::other("injected write failure"));
        }
        let remaining = N.saturating_sub(self.len);
        let written = bytes.len().min(self.max_write).min(remaining);
        if written == 0 {
            return Ok(0);
        }
        self.bytes[self.len..self.len + written].copy_from_slice(&bytes[..written]);
        self.len += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::other("injected flush failure"));
        }
        self.flushes = self.flushes.saturating_add(1);
        Ok(())
    }
}

/// Runs bounded journal pressure, persistence, and recovery faults.
///
/// # Errors
///
/// Returns the first failed scenario check.
pub fn run_journal_faults() -> Result<JournalFaultResult, JournalFaultError> {
    let mut result = run_saturation_and_shutdown()?;
    let (valid_crash_prefixes, rejected_truncations) = run_crash_cuts()?;
    result.valid_crash_prefixes = valid_crash_prefixes;
    result.rejected_truncations = rejected_truncations;
    run_write_failure()?;
    result.hard_write_failures = 1;
    run_flush_failure()?;
    result.hard_flush_failures = 1;
    Ok(result)
}

fn run_saturation_and_shutdown() -> Result<JournalFaultResult, JournalFaultError> {
    let mut channel =
        JournalChannel::try_new().map_err(|_| JournalFaultError::at(JournalFaultCheck::Channel))?;
    let (mut writer, reader) = channel.split(1);
    for sequence in 1..=RING_CAPACITY as u64 {
        let payload = sequence.to_be_bytes();
        if writer.enqueue(&payload) != Ok(SequenceNumber(sequence)) {
            return Err(JournalFaultError::at(JournalFaultCheck::Fill));
        }
    }

    let retry_payload = [0xa5; hft_journal::MAX_PAYLOAD];
    if writer.enqueue(&retry_payload) != Err(JournalError::Saturated) {
        return Err(JournalFaultError::at(JournalFaultCheck::Saturation));
    }
    let retry_sequence = RING_CAPACITY as u64 + 1;
    if writer.next_sequence() != retry_sequence {
        return Err(JournalFaultError::at(
            JournalFaultCheck::SequenceAfterSaturation,
        ));
    }

    let sink = BoundedSink::<RETAINED_BYTES>::new(SHORT_WRITE_LIMIT);
    let mut worker = PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::OnShutdown)
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Worker))?;
    if !matches!(worker.drain_batch(), Ok(1)) {
        return Err(JournalFaultError::at(JournalFaultCheck::Drain));
    }
    if writer.enqueue(&retry_payload) != Ok(SequenceNumber(retry_sequence)) {
        return Err(JournalFaultError::at(JournalFaultCheck::Retry));
    }
    if !matches!(worker.shutdown(), Err(PersistError::ProducerOpen)) {
        return Err(JournalFaultError::at(JournalFaultCheck::ProducerOpen));
    }
    writer.close();
    worker
        .shutdown()
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Shutdown))?;
    let sink = worker
        .into_sink()
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Sink))?;
    let recovery = recover(&mut Cursor::new(sink.slice()), 1)
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Recovery))?;
    if recovery.records != RETAINED_RECORDS as u64
        || recovery.next_sequence != RETAINED_RECORDS as u64 + 1
    {
        return Err(JournalFaultError::at(JournalFaultCheck::Recovery));
    }

    Ok(JournalFaultResult {
        saturation_refusals: 1,
        retry_successes: 1,
        short_write_calls: sink.write_calls,
        producer_open_refusals: 1,
        final_flushes: sink.flushes,
        recovered_records: recovery.records,
        valid_crash_prefixes: 0,
        rejected_truncations: 0,
        hard_write_failures: 0,
        hard_flush_failures: 0,
    })
}

fn run_crash_cuts() -> Result<(u64, u64), JournalFaultError> {
    let first = JournalRecord::new(SequenceNumber(1), b"first")
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::CrashPrefix))?
        .encode();
    let second = JournalRecord::new(SequenceNumber(2), b"second")
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::CrashPrefix))?
        .encode();
    let mut bytes = [0_u8; RECORD_SIZE * 2];
    bytes[..RECORD_SIZE].copy_from_slice(&first);
    bytes[RECORD_SIZE..].copy_from_slice(&second);

    let mut valid = 0_u64;
    let mut truncated = 0_u64;
    for cut in 0..=bytes.len() {
        match recover(&mut Cursor::new(&bytes[..cut]), 1) {
            Ok(recovery) if cut % RECORD_SIZE == 0 => {
                let expected_records = (cut / RECORD_SIZE) as u64;
                if recovery.records != expected_records
                    || recovery.next_sequence != expected_records + 1
                {
                    return Err(JournalFaultError::at(JournalFaultCheck::CrashPrefix));
                }
                valid = valid.saturating_add(1);
            }
            Err(RecoveryError::Truncated { bytes: partial })
                if cut % RECORD_SIZE != 0 && partial == cut % RECORD_SIZE =>
            {
                truncated = truncated.saturating_add(1);
            }
            Ok(_) | Err(_) if cut % RECORD_SIZE == 0 => {
                return Err(JournalFaultError::at(JournalFaultCheck::CrashPrefix));
            }
            Ok(_) | Err(_) => {
                return Err(JournalFaultError::at(JournalFaultCheck::Truncation));
            }
        }
    }
    Ok((valid, truncated))
}

fn run_write_failure() -> Result<(), JournalFaultError> {
    let mut channel =
        JournalChannel::try_new().map_err(|_| JournalFaultError::at(JournalFaultCheck::Channel))?;
    let (mut writer, reader) = channel.split(1);
    writer
        .enqueue(b"write failure")
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::WriteFailure))?;
    writer.close();
    let mut sink = BoundedSink::<RECORD_SIZE>::new(RECORD_SIZE);
    sink.fail_write_call = Some(1);
    let mut worker = PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::EveryBatch)
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Worker))?;
    if !matches!(worker.drain_batch(), Err(PersistError::Io(_))) {
        return Err(JournalFaultError::at(JournalFaultCheck::WriteFailure));
    }
    if !matches!(worker.drain_batch(), Err(PersistError::Poisoned)) {
        return Err(JournalFaultError::at(JournalFaultCheck::Poison));
    }
    Ok(())
}

fn run_flush_failure() -> Result<(), JournalFaultError> {
    let mut channel =
        JournalChannel::try_new().map_err(|_| JournalFaultError::at(JournalFaultCheck::Channel))?;
    let (mut writer, reader) = channel.split(1);
    writer
        .enqueue(b"flush failure")
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::FlushFailure))?;
    writer.close();
    let mut sink = BoundedSink::<RECORD_SIZE>::new(RECORD_SIZE);
    sink.fail_flush = true;
    let mut worker = PersistenceWorker::<_, 1>::new(reader, sink, FlushPolicy::EveryBatch)
        .map_err(|_| JournalFaultError::at(JournalFaultCheck::Worker))?;
    if !matches!(worker.drain_batch(), Err(PersistError::Io(_))) {
        return Err(JournalFaultError::at(JournalFaultCheck::FlushFailure));
    }
    if !matches!(worker.shutdown(), Err(PersistError::Poisoned)) {
        return Err(JournalFaultError::at(JournalFaultCheck::Poison));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_fault_counts_are_exact_and_repeatable() {
        if hft_spsc::IS_LOOM_BUILD {
            return;
        }
        let expected = JournalFaultResult {
            saturation_refusals: 1,
            retry_successes: 1,
            short_write_calls: 10_250,
            producer_open_refusals: 1,
            final_flushes: 1,
            recovered_records: 1_025,
            valid_crash_prefixes: 3,
            rejected_truncations: 126,
            hard_write_failures: 1,
            hard_flush_failures: 1,
        };
        assert_eq!(run_journal_faults(), Ok(expected));
        assert_eq!(run_journal_faults(), Ok(expected));
    }
}
