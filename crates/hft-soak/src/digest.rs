use crate::Seed;
use sha2::{Digest as _, Sha256};
use std::fmt;

const CANONICAL_DOMAIN: &[u8] = b"deterministic-exchange canonical digest v1\0";
const SCENARIO_SEED_DOMAIN: &[u8] = b"deterministic-exchange soak scenario seed v1\0";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Digest([u8; 32]);

impl Digest {
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            let _ = write!(output, "{byte:02x}");
        }
        output
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestError {
    TooManyFields,
    FieldTooLong,
    DomainTooLong,
}

impl fmt::Display for DigestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyFields => formatter.write_str("digest field count exceeds u32"),
            Self::FieldTooLong => formatter.write_str("digest field length exceeds u64"),
            Self::DomainTooLong => formatter.write_str("digest domain length exceeds u32"),
        }
    }
}

impl std::error::Error for DigestError {}

/// Hashes an unframed byte string with SHA-256.
#[must_use]
pub fn sha256(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

/// Hashes a domain and length-prefixed fields with fixed big-endian framing.
///
/// # Errors
///
/// Returns an error when a framing count cannot be represented.
pub fn canonical_sha256(domain: &[u8], fields: &[&[u8]]) -> Result<Digest, DigestError> {
    let domain_len = u32::try_from(domain.len()).map_err(|_| DigestError::DomainTooLong)?;
    let field_count = u32::try_from(fields.len()).map_err(|_| DigestError::TooManyFields)?;
    let mut hasher = Sha256::new();
    hasher.update(CANONICAL_DOMAIN);
    hasher.update(domain_len.to_be_bytes());
    hasher.update(domain);
    hasher.update(field_count.to_be_bytes());
    for field in fields {
        let length = u64::try_from(field.len()).map_err(|_| DigestError::FieldTooLong)?;
        hasher.update(length.to_be_bytes());
        hasher.update(field);
    }
    Ok(Digest(hasher.finalize().into()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    Churn,
    SequenceGaps,
    Reconnect,
    QueuePressure,
    JournalStalls,
    Snapshots,
    Recovery,
    MalformedInput,
    Exhaustion,
    RoutingImbalance,
    ShutdownRaces,
}

impl Scenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Churn => "churn",
            Self::SequenceGaps => "sequence-gaps",
            Self::Reconnect => "reconnect",
            Self::QueuePressure => "queue-pressure",
            Self::JournalStalls => "journal-stalls",
            Self::Snapshots => "snapshots",
            Self::Recovery => "recovery",
            Self::MalformedInput => "malformed-input",
            Self::Exhaustion => "exhaustion",
            Self::RoutingImbalance => "routing-imbalance",
            Self::ShutdownRaces => "shutdown-races",
        }
    }

    const fn domain_tag(self) -> &'static [u8] {
        match self {
            Self::Churn => b"churn-v1",
            Self::SequenceGaps => b"sequence-gaps-v1",
            Self::Reconnect => b"reconnect-v1",
            Self::QueuePressure => b"queue-pressure-v1",
            Self::JournalStalls => b"journal-stalls-v1",
            Self::Snapshots => b"snapshots-v1",
            Self::Recovery => b"recovery-v1",
            Self::MalformedInput => b"malformed-input-v1",
            Self::Exhaustion => b"exhaustion-v1",
            Self::RoutingImbalance => b"routing-imbalance-v1",
            Self::ShutdownRaces => b"shutdown-races-v1",
        }
    }
}

/// Derives a scenario-local seed without sharing a random stream between
/// scenarios.
///
/// # Errors
///
/// Returns an error if canonical digest framing cannot represent its inputs.
pub fn derive_scenario_seed(root: Seed, scenario: Scenario) -> Result<Seed, DigestError> {
    let root_bytes = root.0.to_be_bytes();
    let digest = canonical_sha256(
        SCENARIO_SEED_DOMAIN,
        &[root_bytes.as_slice(), scenario.domain_tag()],
    )?;
    let bytes = digest.bytes();
    let mut seed_bytes = [0_u8; 8];
    seed_bytes.copy_from_slice(&bytes[..8]);
    Ok(Seed(u64::from_be_bytes(seed_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_sha256_matches_standard_vector() {
        assert_eq!(
            sha256(b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn canonical_framing_separates_field_boundaries_and_domains() {
        let split = canonical_sha256(b"state", &[b"a", b"bc"]).expect("valid framing");
        let joined = canonical_sha256(b"state", &[b"abc"]).expect("valid framing");
        let other_domain = canonical_sha256(b"events", &[b"a", b"bc"]).expect("valid framing");
        assert_ne!(split, joined);
        assert_ne!(split, other_domain);
        assert_eq!(
            split,
            canonical_sha256(b"state", &[b"a", b"bc"]).expect("valid framing")
        );
    }

    #[test]
    fn scenario_streams_are_stable_and_separate() {
        let root = Seed(0x5eed_5eed_5eed_5eed);
        let churn = derive_scenario_seed(root, Scenario::Churn).expect("valid derivation");
        let recovery = derive_scenario_seed(root, Scenario::Recovery).expect("valid derivation");
        assert_ne!(churn, recovery);
        assert_eq!(churn, Seed(0x0c3b_173f_015a_b38e));
    }
}
