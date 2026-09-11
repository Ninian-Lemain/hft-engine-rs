//! Seeded fault scenarios, repeat verification and stable soak result records.
#![forbid(unsafe_code)]

pub mod capacity;
pub mod config;
pub mod digest;
pub mod events;
pub mod journal_faults;
pub mod recovery_faults;
pub mod result;
pub mod routed;
pub mod runner;
pub mod session_faults;

pub use config::{
    CliError, CliOptions, ConfigError, Profile, RetainedSeedError, RunConfig, Seed,
    parse_retained_seeds,
};
pub use digest::{Digest, DigestError, Scenario, canonical_sha256, derive_scenario_seed, sha256};
pub use result::{ResultError, RunResult, RunStatus};
pub use runner::{ScenarioResults, SoakError, run, run_verified};

/// Retained root seeds shipped with this format version.
pub const RETAINED_SEEDS_V1: &str = include_str!("../seeds/v1.txt");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_seed_file_is_valid() {
        let seeds = parse_retained_seeds(RETAINED_SEEDS_V1).expect("valid retained seeds");
        assert_eq!(seeds.len(), 4);
    }
}
