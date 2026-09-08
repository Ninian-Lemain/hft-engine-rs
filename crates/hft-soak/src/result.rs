use crate::{Digest, Profile, Seed};
use std::fmt;

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
    pub peak_rss_bytes: u64,
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

    /// Writes one stable-key-order JSON result record.
    ///
    /// # Errors
    ///
    /// Returns an error if result fields are inconsistent.
    pub fn to_json_line(&self) -> Result<String, ResultError> {
        self.validate()?;
        let mut line = String::with_capacity(384);
        line.push_str("{\"schema\":\"hft-soak-results/1\",\"profile\":\"");
        line.push_str(self.profile.as_str());
        line.push_str("\",\"seed\":\"");
        line.push_str(&self.seed.to_hex());
        line.push_str("\",\"steps\":");
        line.push_str(&self.steps.to_string());
        line.push_str(",\"completed_steps\":");
        line.push_str(&self.completed_steps.to_string());
        line.push_str(",\"status\":\"");
        line.push_str(self.status.as_str());
        line.push_str("\",\"state_digest\":\"");
        line.push_str(&self.state_digest.to_hex());
        line.push_str("\",\"event_digest\":\"");
        line.push_str(&self.event_digest.to_hex());
        line.push_str("\",\"peak_rss_bytes\":");
        line.push_str(&self.peak_rss_bytes.to_string());
        line.push_str(",\"failure\":");
        match &self.failure {
            Some(failure) => push_json_string(&mut line, failure),
            None => line.push_str("null"),
        }
        line.push('}');
        Ok(line)
    }
}

fn push_json_string(output: &mut String, value: &str) {
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
    use crate::sha256;

    fn result(status: RunStatus, failure: Option<String>) -> RunResult {
        RunResult {
            profile: Profile::Smoke,
            seed: Seed(1),
            steps: 10,
            completed_steps: if status == RunStatus::Passed { 10 } else { 7 },
            status,
            state_digest: sha256(b"state"),
            event_digest: sha256(b"events"),
            peak_rss_bytes: 4_096,
            failure,
        }
    }

    #[test]
    fn passed_result_has_stable_key_order() {
        let line = result(RunStatus::Passed, None)
            .to_json_line()
            .expect("valid result");
        assert_eq!(
            line,
            concat!(
                "{\"schema\":\"hft-soak-results/1\",",
                "\"profile\":\"smoke\",",
                "\"seed\":\"0000000000000001\",",
                "\"steps\":10,",
                "\"completed_steps\":10,",
                "\"status\":\"passed\",",
                "\"state_digest\":\"4ba69735ca53765ed6a709edb56c6ea23",
                "6b7193a3b29a6b390c346f0f4340e4e\",",
                "\"event_digest\":\"862417b9e7c3720bcb3263cd873b0989",
                "2d787823b6f9a0f453e42824c5a4d4b6\",",
                "\"peak_rss_bytes\":4096,",
                "\"failure\":null}"
            )
        );
    }

    #[test]
    fn failure_text_is_json_escaped() {
        let line = result(RunStatus::Failed, Some("queue \"full\"\nretry".to_owned()))
            .to_json_line()
            .expect("valid result");
        assert!(line.ends_with("\"failure\":\"queue \\\"full\\\"\\nretry\"}"));
    }

    #[test]
    fn inconsistent_results_are_rejected() {
        let mut value = result(RunStatus::Passed, None);
        value.completed_steps = 9;
        assert_eq!(value.validate(), Err(ResultError::PassedBeforeCompletion));

        let value = result(RunStatus::Failed, None);
        assert_eq!(value.validate(), Err(ResultError::FailedWithoutFailure));

        let mut value = result(RunStatus::Passed, None);
        value.steps = 0;
        value.completed_steps = 0;
        assert_eq!(value.validate(), Err(ResultError::ZeroSteps));
    }
}
