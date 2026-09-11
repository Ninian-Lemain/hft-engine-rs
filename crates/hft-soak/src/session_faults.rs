use crate::Seed;
use hft_session::retransmit::{MAX_FRAME, RetainError, RetransmitBuffer};
use hft_session::{
    SessionConfig, SessionError, SessionEvent, SessionState, SessionStateMachine, Transition,
};
use hft_types::SequenceNumber;
use std::fmt;

const RETRANSMIT_CAPACITY: usize = 2;
const HEARTBEAT_TICKS: u64 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionFaultResult {
    pub steps: u64,
    pub accepted_commands: u64,
    pub gaps_rejected: u64,
    pub duplicates_rejected: u64,
    pub heartbeat_timeouts: u64,
    pub reconnects: u64,
    pub retransmit_full: u64,
    pub idempotent_confirms: u64,
    pub digest: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionFaultError {
    ZeroSteps,
    ArithmeticOverflow,
    Session {
        stage: &'static str,
        source: SessionError,
    },
    Retain {
        stage: &'static str,
        source: RetainError,
    },
    Invariant(&'static str),
}

impl fmt::Display for SessionFaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSteps => formatter.write_str("session fault steps must be greater than zero"),
            Self::ArithmeticOverflow => formatter.write_str("session fault counter overflow"),
            Self::Session { stage, source } => {
                write!(formatter, "session fault at {stage}: {source:?}")
            }
            Self::Retain { stage, source } => {
                write!(formatter, "retransmit fault at {stage}: {source}")
            }
            Self::Invariant(name) => write!(formatter, "session fault invariant failed: {name}"),
        }
    }
}

impl std::error::Error for SessionFaultError {}

/// Runs bounded session faults with caller supplied virtual time.
///
/// # Errors
///
/// Returns the first unexpected transition, retransmit result, or counter
/// overflow.
pub fn run_session_faults(seed: Seed, steps: u64) -> Result<SessionFaultResult, SessionFaultError> {
    if steps == 0 {
        return Err(SessionFaultError::ZeroSteps);
    }

    let mut result = SessionFaultResult {
        steps,
        accepted_commands: 0,
        gaps_rejected: 0,
        duplicates_rejected: 0,
        heartbeat_timeouts: 0,
        reconnects: 0,
        retransmit_full: 0,
        idempotent_confirms: 0,
        digest: seed.0 ^ 0x7365_7373_696f_6e31,
    };

    for step in 0..steps {
        run_step(seed, step, &mut result)?;
    }
    Ok(result)
}

fn run_step(
    seed: Seed,
    step: u64,
    result: &mut SessionFaultResult,
) -> Result<(), SessionFaultError> {
    let config = SessionConfig {
        logon_timeout_ticks: 20,
        heartbeat_timeout_ticks: HEARTBEAT_TICKS,
    };
    let mut session = SessionStateMachine::new(config);
    activate(&mut session, SequenceNumber(1), 0)?;

    expect_transition(
        &mut session,
        SessionEvent::Command {
            sequence: SequenceNumber(1),
        },
        1,
        SessionState::Active,
        "first command",
    )?;
    increment(&mut result.accepted_commands)?;

    exercise_sequence_refusals(&mut session, result)?;
    exercise_retransmit(seed, step, result)?;

    let timeout =
        session
            .tick(1 + HEARTBEAT_TICKS)
            .map_err(|source| SessionFaultError::Session {
                stage: "heartbeat timeout",
                source,
            })?;
    if timeout.state != SessionState::Recovering || !session.allows_commands() {
        return Err(SessionFaultError::Invariant(
            "timeout did not enter recovery",
        ));
    }
    increment(&mut result.heartbeat_timeouts)?;

    expect_transition(
        &mut session,
        SessionEvent::Command {
            sequence: SequenceNumber(2),
        },
        7,
        SessionState::Active,
        "recovery command",
    )?;
    increment(&mut result.accepted_commands)?;

    let resume_at = session.expected_sequence();
    expect_transition(
        &mut session,
        SessionEvent::Disconnect,
        8,
        SessionState::Disconnected,
        "disconnect",
    )?;
    activate(&mut session, resume_at, 9)?;
    if session.expected_sequence() != resume_at {
        return Err(SessionFaultError::Invariant(
            "logon changed resume sequence",
        ));
    }
    increment(&mut result.reconnects)?;

    expect_transition(
        &mut session,
        SessionEvent::Command {
            sequence: resume_at,
        },
        10,
        SessionState::Active,
        "resumed command",
    )?;
    increment(&mut result.accepted_commands)?;

    result.digest = mix(result.digest, step);
    result.digest = mix(result.digest, session.expected_sequence().0);
    Ok(())
}

fn exercise_sequence_refusals(
    session: &mut SessionStateMachine,
    result: &mut SessionFaultResult,
) -> Result<(), SessionFaultError> {
    let expected = session.expected_sequence();
    let deadline = session.deadline();
    match session.handle(
        SessionEvent::Command {
            sequence: SequenceNumber(3),
        },
        2,
    ) {
        Err(SessionError::Gap {
            expected: SequenceNumber(2),
            received: SequenceNumber(3),
        }) => {}
        Err(source) => {
            return Err(SessionFaultError::Session {
                stage: "sequence gap",
                source,
            });
        }
        Ok(_) => return Err(SessionFaultError::Invariant("gap accepted")),
    }
    if session.expected_sequence() != expected || session.deadline() != deadline {
        return Err(SessionFaultError::Invariant("gap changed session"));
    }
    increment(&mut result.gaps_rejected)?;

    match session.handle(
        SessionEvent::Command {
            sequence: SequenceNumber(1),
        },
        2,
    ) {
        Err(SessionError::DuplicateSequence {
            received: SequenceNumber(1),
            expected: SequenceNumber(2),
        }) => {}
        Err(source) => {
            return Err(SessionFaultError::Session {
                stage: "duplicate sequence",
                source,
            });
        }
        Ok(_) => return Err(SessionFaultError::Invariant("duplicate accepted")),
    }
    if session.expected_sequence() != expected || session.deadline() != deadline {
        return Err(SessionFaultError::Invariant("duplicate changed session"));
    }
    increment(&mut result.duplicates_rejected)?;

    Ok(())
}

fn exercise_retransmit(
    seed: Seed,
    step: u64,
    result: &mut SessionFaultResult,
) -> Result<(), SessionFaultError> {
    let first = frame_bytes(seed, step, 1);
    let second = frame_bytes(seed, step, 2);
    let third = frame_bytes(seed, step, 3);
    let mut retransmit = RetransmitBuffer::new(RETRANSMIT_CAPACITY);
    retain(&mut retransmit, SequenceNumber(1), &first, "retain one")?;
    retain(&mut retransmit, SequenceNumber(2), &second, "retain two")?;
    match retransmit.retain(SequenceNumber(3), &third) {
        Err(RetainError::Full) => {}
        Err(source) => {
            return Err(SessionFaultError::Retain {
                stage: "full buffer",
                source,
            });
        }
        Ok(()) => return Err(SessionFaultError::Invariant("full buffer accepted frame")),
    }
    increment(&mut result.retransmit_full)?;
    assert_since_bytes(&retransmit, [&first, &second])?;

    if retransmit.confirm_through(1) != 1 || retransmit.confirm_through(1) != 0 {
        return Err(SessionFaultError::Invariant(
            "confirmation was not idempotent",
        ));
    }
    increment(&mut result.idempotent_confirms)?;

    for byte in first.iter().chain(second.iter()).chain(third.iter()) {
        result.digest = mix(result.digest, u64::from(*byte));
    }
    Ok(())
}

fn activate(
    session: &mut SessionStateMachine,
    first_sequence: SequenceNumber,
    now: u64,
) -> Result<(), SessionFaultError> {
    expect_transition(
        session,
        SessionEvent::Connect,
        now,
        SessionState::Connecting,
        "connect",
    )?;
    expect_transition(
        session,
        SessionEvent::LogonSent,
        now,
        SessionState::Logon,
        "logon sent",
    )?;
    expect_transition(
        session,
        SessionEvent::LogonAccepted { first_sequence },
        now,
        SessionState::Active,
        "logon accepted",
    )?;
    Ok(())
}

fn expect_transition(
    session: &mut SessionStateMachine,
    event: SessionEvent,
    now: u64,
    expected: SessionState,
    stage: &'static str,
) -> Result<Transition, SessionFaultError> {
    let transition = session
        .handle(event, now)
        .map_err(|source| SessionFaultError::Session { stage, source })?;
    if transition.state != expected {
        return Err(SessionFaultError::Invariant(stage));
    }
    Ok(transition)
}

fn retain(
    buffer: &mut RetransmitBuffer,
    sequence: SequenceNumber,
    bytes: &[u8],
    stage: &'static str,
) -> Result<(), SessionFaultError> {
    buffer
        .retain(sequence, bytes)
        .map_err(|source| SessionFaultError::Retain { stage, source })
}

fn assert_since_bytes(
    buffer: &RetransmitBuffer,
    expected: [&[u8; MAX_FRAME]; RETRANSMIT_CAPACITY],
) -> Result<(), SessionFaultError> {
    if buffer.since(1).count() != RETRANSMIT_CAPACITY {
        return Err(SessionFaultError::Invariant("retransmit suffix length"));
    }
    for (expected_sequence, (frame, bytes)) in (1_u64..).zip(buffer.since(1).zip(expected)) {
        if frame.sequence.0 != expected_sequence || frame.slice() != bytes {
            return Err(SessionFaultError::Invariant("retransmit bytes changed"));
        }
    }
    Ok(())
}

fn frame_bytes(seed: Seed, step: u64, sequence: u64) -> [u8; MAX_FRAME] {
    let mut bytes = [0_u8; MAX_FRAME];
    let mut state = seed.0 ^ step.rotate_left(17) ^ sequence.rotate_left(41);
    for byte in &mut bytes {
        state = mix(state, sequence);
        *byte = state.to_be_bytes()[0];
    }
    bytes
}

fn increment(value: &mut u64) -> Result<(), SessionFaultError> {
    *value = value
        .checked_add(1)
        .ok_or(SessionFaultError::ArithmeticOverflow)?;
    Ok(())
}

const fn mix(state: u64, value: u64) -> u64 {
    state
        .rotate_left(13)
        .wrapping_add(value ^ 0x9e37_79b9_7f4a_7c15)
        .wrapping_mul(0xbf58_476d_1ce4_e5b9)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_is_repeatable_and_covers_each_fault() {
        let first = run_session_faults(Seed(0x1234), 3).expect("session faults");
        let second = run_session_faults(Seed(0x1234), 3).expect("session faults");
        assert_eq!(first, second);
        assert_eq!(first.accepted_commands, 9);
        assert_eq!(first.gaps_rejected, 3);
        assert_eq!(first.duplicates_rejected, 3);
        assert_eq!(first.heartbeat_timeouts, 3);
        assert_eq!(first.reconnects, 3);
        assert_eq!(first.retransmit_full, 3);
        assert_eq!(first.idempotent_confirms, 3);
    }
}
