use crate::capacity::{CapacityResult, run_capacity};
use crate::journal_faults::{JournalFaultResult, run_journal_faults};
use crate::recovery_faults::{RecoveryFaultResult, run_recovery_faults};
use crate::result::push_json_string;
use crate::routed::{self, RoutedResult};
use crate::session_faults::{SessionFaultResult, run_session_faults};
use crate::{RunConfig, RunResult, RunStatus, Scenario, canonical_sha256, derive_scenario_seed};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioResults {
    pub routed: RoutedResult,
    pub session: SessionFaultResult,
    pub recovery: RecoveryFaultResult,
    pub journal: JournalFaultResult,
    pub capacity: CapacityResult,
}

/// The first failing phase, with the original configuration needed to replay it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakError {
    pub config: RunConfig,
    pub phase: &'static str,
    pub message: String,
}

impl SoakError {
    fn at(config: RunConfig, phase: &'static str, source: impl fmt::Display) -> Self {
        Self {
            config,
            phase,
            message: source.to_string(),
        }
    }

    #[must_use]
    pub fn replay_command(&self) -> String {
        format!(
            "cargo run --release -p hft-soak -- --profile {} --seed {} --steps {}",
            self.config.profile, self.config.seed, self.config.steps
        )
    }

    #[must_use]
    pub fn to_json_line(&self) -> String {
        let mut line = format!(
            "{{\"schema\":\"hft-soak-error/1\",\"profile\":\"{}\",\"seed\":\"{}\",\"steps\":{},\"status\":\"failed\",\"phase\":",
            self.config.profile, self.config.seed, self.config.steps
        );
        push_json_string(&mut line, self.phase);
        line.push_str(",\"failure\":");
        push_json_string(&mut line, &self.message);
        line.push_str(",\"replay_command\":");
        push_json_string(&mut line, &self.replay_command());
        line.push('}');
        line
    }
}

impl fmt::Display for SoakError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "seed {} failed in {}: {}. Replay with {}",
            self.config.seed,
            self.phase,
            self.message,
            self.replay_command()
        )
    }
}

impl std::error::Error for SoakError {}

/// Runs all scenarios once, stopping at the first failure.
///
/// Routed and recovery traffic each receive `config.steps`. Session faults run
/// once per 64 steps, rounded up. Journal and capacity fixtures run once each.
///
/// # Errors
///
/// Returns invalid configuration or the first scenario or digest failure.
pub fn run(config: RunConfig) -> Result<RunResult, SoakError> {
    RunConfig::new(config.profile, config.seed, config.steps)
        .map_err(|error| SoakError::at(config, "configuration", error))?;
    let seed_for = |scenario| {
        derive_scenario_seed(config.seed, scenario)
            .map_err(|error| SoakError::at(config, "seed derivation", error))
    };
    let routed = routed::run(seed_for(Scenario::Churn)?.0, config.steps)
        .map_err(|error| SoakError::at(config, "routed", error))?;
    let session = run_session_faults(seed_for(Scenario::Reconnect)?, config.steps.div_ceil(64))
        .map_err(|error| SoakError::at(config, "session", error))?;
    let recovery = run_recovery_faults(seed_for(Scenario::Recovery)?, config.steps)
        .map_err(|error| SoakError::at(config, "recovery", error))?;
    let journal = run_journal_faults().map_err(|error| SoakError::at(config, "journal", error))?;
    let capacity = run_capacity().map_err(|error| SoakError::at(config, "capacity", error))?;
    let scenarios = ScenarioResults {
        routed,
        session,
        recovery,
        journal,
        capacity,
    };
    let state_digest = canonical_sha256(
        b"soak-state-v1",
        &[
            &routed.state_fingerprint.to_be_bytes(),
            &recovery.snapshot_digest,
            &session.digest.to_be_bytes(),
        ],
    )
    .map_err(|error| SoakError::at(config, "state digest", error))?;
    let mut counters = String::new();
    scenarios.push_json(&mut counters);
    let event_digest = canonical_sha256(
        b"soak-events-v1",
        &[&routed.event_fingerprint.to_be_bytes(), counters.as_bytes()],
    )
    .map_err(|error| SoakError::at(config, "event digest", error))?;
    let result = RunResult {
        profile: config.profile,
        seed: config.seed,
        steps: config.steps,
        completed_steps: config.steps,
        status: RunStatus::Passed,
        state_digest,
        event_digest,
        scenarios,
        peak_rss_bytes: None,
        failure: None,
    };
    result
        .validate()
        .map_err(|error| SoakError::at(config, "result", error))?;
    Ok(result)
}

/// Runs the full suite twice and compares deterministic state, events and counts.
///
/// Resource measurements are excluded from comparison.
///
/// # Errors
///
/// Returns the first run failure or a difference between the two results.
pub fn run_verified(config: RunConfig) -> Result<RunResult, SoakError> {
    let first = run(config)?;
    let repeated = run(config)?;
    if !first.deterministic_eq(&repeated) {
        return Err(SoakError::at(
            config,
            "repeat verification",
            "same seed produced different results",
        ));
    }
    Ok(first)
}
