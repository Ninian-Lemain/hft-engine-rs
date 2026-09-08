//! Deterministic configuration and result records for fault and soak runs.
#![forbid(unsafe_code)]

pub mod config;
pub mod digest;
pub mod result;

pub use config::{
    CliError, CliOptions, ConfigError, Profile, RetainedSeedError, RunConfig, Seed,
    parse_retained_seeds,
};
pub use digest::{Digest, DigestError, Scenario, canonical_sha256, derive_scenario_seed, sha256};
pub use result::{ResultError, RunResult, RunStatus};

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
