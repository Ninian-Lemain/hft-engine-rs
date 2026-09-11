use crate::{Digest, Profile, ScenarioResults, Seed};
use std::fmt;
use std::fmt::Write as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Passed,
    Failed,
}

impl RunStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultError {
    ZeroSteps,
    CompletedStepsExceedDeclared,
    PassedBeforeCompletion,
    PassedWithFailure,
    FailedWithoutFailure,
    ScenarioStepsMismatch,
}

impl fmt::Display for ResultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSteps => formatter.write_str("steps must be greater than zero"),
            Self::CompletedStepsExceedDeclared => {
                formatter.write_str("completed steps exceed declared steps")
            }
            Self::PassedBeforeCompletion => {
                formatter.write_str("passed result did not complete all declared steps")
            }
            Self::PassedWithFailure => formatter.write_str("passed result contains a failure"),
            Self::FailedWithoutFailure => formatter.write_str("failed result has no failure text"),
            Self::ScenarioStepsMismatch => {
                formatter.write_str("scenario steps do not match the declared work")
            }
        }
    }
}

impl std::error::Error for ResultError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunResult {
    pub profile: Profile,
    pub seed: Seed,
    pub steps: u64,
    pub completed_steps: u64,
    pub status: RunStatus,
    pub state_digest: Digest,
    pub event_digest: Digest,
    pub scenarios: ScenarioResults,
    pub peak_rss_bytes: Option<u64>,
    pub failure: Option<String>,
}

impl RunResult {
    /// Validates result completion and failure fields.
    ///
    /// # Errors
    ///
    /// Returns an error when the status contradicts completion or failure data.
    pub fn validate(&self) -> Result<(), ResultError> {
        if self.steps == 0 {
            return Err(ResultError::ZeroSteps);
        }
        if self.completed_steps > self.steps {
            return Err(ResultError::CompletedStepsExceedDeclared);
        }
        if self.status == RunStatus::Passed
            && (self.scenarios.routed.steps != self.steps
                || self.scenarios.recovery.steps != self.steps
                || self.scenarios.recovery.commands != self.steps
                || self.scenarios.session.steps != self.steps.div_ceil(64))
        {
            return Err(ResultError::ScenarioStepsMismatch);
        }
        match (
            self.status,
            self.completed_steps == self.steps,
            &self.failure,
        ) {
            (RunStatus::Passed, false, _) => Err(ResultError::PassedBeforeCompletion),
            (RunStatus::Passed, true, Some(_)) => Err(ResultError::PassedWithFailure),
            (RunStatus::Failed, _, None) => Err(ResultError::FailedWithoutFailure),
            _ => Ok(()),
        }
    }

    /// Compares deterministic fields, excluding resource measurements.
    #[must_use]
    pub fn deterministic_eq(&self, other: &Self) -> bool {
        self.profile == other.profile
            && self.seed == other.seed
            && self.steps == other.steps
            && self.completed_steps == other.completed_steps
            && self.status == other.status
            && self.state_digest == other.state_digest
            && self.event_digest == other.event_digest
            && self.scenarios == other.scenarios
            && self.failure == other.failure
    }

    /// Writes one stable-key-order JSON result record.
    ///
    /// # Errors
    ///
    /// Returns an error if result fields are inconsistent.
    pub fn to_json_line(&self) -> Result<String, ResultError> {
        self.validate()?;
        let mut line = String::with_capacity(2_048);
        let _ = write!(
            line,
            concat!(
                "{{\"schema\":\"hft-soak-results/1\",\"profile\":\"{}\",",
                "\"seed\":\"{}\",\"steps\":{},\"completed_steps\":{},",
                "\"status\":\"{}\",\"state_digest\":\"{}\",\"event_digest\":\"{}\",",
                "\"scenarios\":"
            ),
            self.profile,
            self.seed,
            self.steps,
            self.completed_steps,
            self.status.as_str(),
            self.state_digest,
            self.event_digest,
        );
        self.scenarios.push_json(&mut line);
        line.push_str(",\"peak_rss_bytes\":");
        match self.peak_rss_bytes {
            Some(bytes) => {
                let _ = write!(line, "{bytes}");
            }
            None => line.push_str("null"),
        }
        line.push_str(",\"failure\":");
        match &self.failure {
            Some(failure) => push_json_string(&mut line, failure),
            None => line.push_str("null"),
        }
        line.push('}');
        Ok(line)
    }
}

impl ScenarioResults {
    pub(crate) fn push_json(&self, output: &mut String) {
        macro_rules! counters {
            ($name:ident, $($field:ident),+ $(,)?) => {{
                output.push_str(concat!("\"", stringify!($name), "\":{"));
                $(let _ = write!(output, concat!("\"", stringify!($field), "\":{},"), self.$name.$field);)+
                output.pop();
                output.push('}');
            }};
        }
        output.push('{');
        counters!(
            routed,
            steps,
            events,
            terminal_events,
            accepted_events,
            rejected_events,
            cancelled_events,
            replaced_events,
            trade_events,
            top_of_book_events,
            command_backpressure,
            event_backpressure,
            pressure_rounds,
            pending_retries,
            last_pressure_step,
            live_order_checks,
            late_steps,
            late_accepted_events,
            late_rejected_events,
            late_cancelled_events,
            late_replaced_events,
            late_trade_events,
            sequence_gaps,
            malformed_frames,
            unknown_instruments
        );
        output.pop();
        let [a, b, c, d] = self.routed.routed_by_shard;
        let _ = write!(output, ",\"routed_by_shard\":[{a},{b},{c},{d}]}},");
        counters!(
            session,
            steps,
            accepted_commands,
            gaps_rejected,
            duplicates_rejected,
            heartbeat_timeouts,
            reconnects,
            retransmit_full,
            idempotent_confirms
        );
        output.push(',');
        counters!(
            recovery,
            steps,
            commands,
            business_rejections,
            accepted_new_orders,
            accepted_cancels,
            accepted_replaces,
            late_accepted_cancels,
            late_accepted_replaces,
            resumed_commands,
            checkpoints,
            fault_checks,
            journal_peak_bytes
        );
        output.push(',');
        counters!(
            journal,
            saturation_refusals,
            retry_successes,
            short_write_calls,
            producer_open_refusals,
            final_flushes,
            recovered_records,
            valid_crash_prefixes,
            rejected_truncations,
            hard_write_failures,
            hard_flush_failures
        );
        output.push(',');
        counters!(
            capacity,
            price_level_order_refusals,
            price_level_order_retries,
            price_level_refusals,
            price_level_retries,
            risk_order_refusals,
            risk_order_retries,
            account_registration_refusals,
            report_refusals,
            report_retries,
            retransmit_refusals,
            retransmit_retries,
            command_queue_refusals,
            command_queue_retries,
            event_queue_refusals,
            event_queue_retries
        );
        output.push('}');
    }
}

pub(crate) fn push_json_string(output: &mut String, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            control if control <= '\u{1f}' => {
                let value = u32::from(control);
                output.push_str("\\u00");
                output.push(char::from(HEX[((value >> 4) & 0x0f) as usize]));
                output.push(char::from(HEX[(value & 0x0f) as usize]));
            }
            other => output.push(other),
        }
    }
    output.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_text_is_json_escaped() {
        let mut output = String::new();
        push_json_string(&mut output, "queue \"full\"\nretry\u{0001}\\");
        assert_eq!(output, "\"queue \\\"full\\\"\\nretry\\u0001\\\\\"");
    }
}
