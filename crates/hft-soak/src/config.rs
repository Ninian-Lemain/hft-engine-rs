use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

const SEED_HEX_LEN: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    Smoke,
    Nightly,
    Qualification,
}

impl Profile {
    #[must_use]
    pub const fn default_steps(self) -> u64 {
        match self {
            Self::Smoke => 10_000,
            Self::Nightly => 10_000_000,
            Self::Qualification => 100_000_000,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Nightly => "nightly",
            Self::Qualification => "qualification",
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileParseError(String);

impl fmt::Display for ProfileParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown profile '{}'. Expected smoke, nightly, or qualification",
            self.0
        )
    }
}

impl std::error::Error for ProfileParseError {}

impl FromStr for Profile {
    type Err = ProfileParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "nightly" => Ok(Self::Nightly),
            "qualification" => Ok(Self::Qualification),
            _ => Err(ProfileParseError(value.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Seed(pub u64);

impl Seed {
    #[must_use]
    pub fn to_hex(self) -> String {
        format!("{:016x}", self.0)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SeedParseError {
    Length { found: usize },
    InvalidCharacter { index: usize, byte: u8 },
}

impl fmt::Display for SeedParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length { found } => write!(
                formatter,
                "seed must contain {SEED_HEX_LEN} lowercase hex digits; found {found}"
            ),
            Self::InvalidCharacter { index, byte } => write!(
                formatter,
                "seed contains invalid byte 0x{byte:02x} at offset {index}"
            ),
        }
    }
}

impl std::error::Error for SeedParseError {}

impl FromStr for Seed {
    type Err = SeedParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != SEED_HEX_LEN {
            return Err(SeedParseError::Length { found: value.len() });
        }
        for (index, byte) in value.bytes().enumerate() {
            if !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte) {
                return Err(SeedParseError::InvalidCharacter { index, byte });
            }
        }
        u64::from_str_radix(value, 16)
            .map(Self)
            .map_err(|_| SeedParseError::Length { found: value.len() })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedSeedError {
    Invalid { line: usize, source: SeedParseError },
    Duplicate { line: usize, seed: Seed },
    Empty,
}

impl fmt::Display for RetainedSeedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { line, source } => {
                write!(formatter, "invalid retained seed on line {line}: {source}")
            }
            Self::Duplicate { line, seed } => {
                write!(formatter, "duplicate retained seed {seed} on line {line}")
            }
            Self::Empty => formatter.write_str("retained seed file contains no seeds"),
        }
    }
}

impl std::error::Error for RetainedSeedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid { source, .. } => Some(source),
            Self::Duplicate { .. } | Self::Empty => None,
        }
    }
}

/// Parses one lowercase 64-bit hex seed per line. Empty lines and text after
/// `#` are ignored.
///
/// # Errors
///
/// Returns the first invalid or duplicate entry. An empty file is rejected.
pub fn parse_retained_seeds(input: &str) -> Result<Vec<Seed>, RetainedSeedError> {
    let mut seeds = Vec::new();
    for (line_index, raw_line) in input.lines().enumerate() {
        let value = raw_line
            .split_once('#')
            .map_or(raw_line, |(value, _)| value)
            .trim();
        if value.is_empty() {
            continue;
        }
        let line = line_index + 1;
        let seed = value
            .parse::<Seed>()
            .map_err(|source| RetainedSeedError::Invalid { line, source })?;
        if seeds.contains(&seed) {
            return Err(RetainedSeedError::Duplicate { line, seed });
        }
        seeds.push(seed);
    }
    if seeds.is_empty() {
        return Err(RetainedSeedError::Empty);
    }
    Ok(seeds)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroSteps,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSteps => formatter.write_str("steps must be greater than zero"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunConfig {
    pub profile: Profile,
    pub seed: Seed,
    pub steps: u64,
}

impl RunConfig {
    /// Builds a validated run configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroSteps`] when no work was requested.
    pub const fn new(profile: Profile, seed: Seed, steps: u64) -> Result<Self, ConfigError> {
        if steps == 0 {
            return Err(ConfigError::ZeroSteps);
        }
        Ok(Self {
            profile,
            seed,
            steps,
        })
    }

    /// Writes a stable-key-order JSON plan record.
    #[must_use]
    pub fn to_json_line(self) -> String {
        format!(
            concat!(
                "{{\"schema\":\"hft-soak-plan/1\",",
                "\"profile\":\"{}\",",
                "\"seed\":\"{}\",",
                "\"steps\":{}",
                "}}"
            ),
            self.profile, self.seed, self.steps
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliOptions {
    pub profile: Profile,
    pub seed: Option<Seed>,
    pub steps: u64,
    pub seed_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliError {
    MissingValue { option: String },
    DuplicateOption { option: String },
    UnknownOption(String),
    Profile(ProfileParseError),
    Seed(SeedParseError),
    InvalidSteps(String),
    ConflictingSeedSources,
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue { option } => write!(formatter, "missing value for {option}"),
            Self::DuplicateOption { option } => write!(formatter, "duplicate option {option}"),
            Self::UnknownOption(option) => write!(formatter, "unknown option {option}"),
            Self::Profile(source) => source.fmt(formatter),
            Self::Seed(source) => source.fmt(formatter),
            Self::InvalidSteps(value) => write!(
                formatter,
                "invalid steps '{value}'. Expected a positive decimal integer"
            ),
            Self::ConflictingSeedSources => {
                formatter.write_str("--seed and --seed-file cannot be used together")
            }
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Profile(source) => Some(source),
            Self::Seed(source) => Some(source),
            _ => None,
        }
    }
}

impl CliOptions {
    /// Parses soak command options without a program-name argument.
    ///
    /// # Errors
    ///
    /// Returns an explicit error for missing, repeated, conflicting, or invalid
    /// options.
    pub fn parse<I, S>(args: I) -> Result<Self, CliError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut profile = None;
        let mut seed = None;
        let mut steps = None;
        let mut seed_file = None;
        let mut args = args.into_iter().map(Into::into);

        while let Some(option) = args.next() {
            match option.as_str() {
                "--profile" => {
                    let value = next_value(&mut args, &option)?;
                    set_once(
                        &mut profile,
                        value.parse().map_err(CliError::Profile)?,
                        &option,
                    )?;
                }
                "--seed" => {
                    let value = next_value(&mut args, &option)?;
                    set_once(&mut seed, value.parse().map_err(CliError::Seed)?, &option)?;
                }
                "--steps" => {
                    let value = next_value(&mut args, &option)?;
                    let parsed = value
                        .parse::<u64>()
                        .map_err(|_| CliError::InvalidSteps(value.clone()))?;
                    if parsed == 0 {
                        return Err(CliError::InvalidSteps(value));
                    }
                    set_once(&mut steps, parsed, &option)?;
                }
                "--seed-file" => {
                    let value = next_value(&mut args, &option)?;
                    if value.is_empty() {
                        return Err(CliError::MissingValue { option });
                    }
                    set_once(&mut seed_file, PathBuf::from(value), &option)?;
                }
                _ => return Err(CliError::UnknownOption(option)),
            }
        }

        if seed.is_some() && seed_file.is_some() {
            return Err(CliError::ConflictingSeedSources);
        }
        let profile = profile.unwrap_or(Profile::Smoke);
        Ok(Self {
            profile,
            seed,
            steps: steps.unwrap_or(profile.default_steps()),
            seed_file,
        })
    }
}

fn next_value<I>(args: &mut I, option: &str) -> Result<String, CliError>
where
    I: Iterator<Item = String>,
{
    args.next().ok_or_else(|| CliError::MissingValue {
        option: option.to_owned(),
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, option: &str) -> Result<(), CliError> {
    if slot.is_some() {
        return Err(CliError::DuplicateOption {
            option: option.to_owned(),
        });
    }
    *slot = Some(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_have_fixed_nonzero_defaults() {
        assert_eq!(Profile::Smoke.default_steps(), 10_000);
        assert_eq!(Profile::Nightly.default_steps(), 10_000_000);
        assert_eq!(Profile::Qualification.default_steps(), 100_000_000);
        for name in ["smoke", "nightly", "qualification"] {
            let profile = name.parse::<Profile>().expect("known profile");
            assert_eq!(profile.as_str(), name);
        }
    }

    #[test]
    fn retained_seeds_accept_comments_and_blank_lines() {
        let input = "# fixture\n0000000000000001 # first\n\ndeadbeefcafef00d\n";
        assert_eq!(
            parse_retained_seeds(input),
            Ok(vec![Seed(1), Seed(0xdead_beef_cafe_f00d)])
        );
    }

    #[test]
    fn retained_seeds_reject_uppercase_and_duplicates() {
        assert!(matches!(
            parse_retained_seeds("000000000000000A\n"),
            Err(RetainedSeedError::Invalid { line: 1, .. })
        ));
        assert_eq!(
            parse_retained_seeds("0000000000000001\n0000000000000001\n"),
            Err(RetainedSeedError::Duplicate {
                line: 2,
                seed: Seed(1)
            })
        );
    }

    #[test]
    fn cli_applies_profile_default_and_overrides() {
        let options = CliOptions::parse([
            "--profile",
            "nightly",
            "--seed",
            "deadbeefcafef00d",
            "--steps",
            "27",
        ])
        .expect("valid options");
        assert_eq!(options.profile, Profile::Nightly);
        assert_eq!(options.seed, Some(Seed(0xdead_beef_cafe_f00d)));
        assert_eq!(options.steps, 27);
        assert_eq!(options.seed_file, None);

        let defaulted =
            CliOptions::parse(["--profile", "qualification"]).expect("valid default configuration");
        assert_eq!(defaulted.steps, 100_000_000);
    }

    #[test]
    fn cli_rejects_conflicts_and_incomplete_options() {
        assert_eq!(
            CliOptions::parse(["--seed", "0000000000000001", "--seed-file", "seeds.txt",]),
            Err(CliError::ConflictingSeedSources)
        );
        assert_eq!(
            CliOptions::parse(["--steps"]),
            Err(CliError::MissingValue {
                option: "--steps".to_owned()
            })
        );
        assert_eq!(
            CliOptions::parse(["--steps", "0"]),
            Err(CliError::InvalidSteps("0".to_owned()))
        );
        assert_eq!(
            CliOptions::parse(["--bogus"]),
            Err(CliError::UnknownOption("--bogus".to_owned()))
        );
    }

    #[test]
    fn plan_json_has_stable_key_order() {
        let config = RunConfig::new(Profile::Smoke, Seed(1), 10).expect("valid config");
        assert_eq!(
            config.to_json_line(),
            concat!(
                "{\"schema\":\"hft-soak-plan/1\",",
                "\"profile\":\"smoke\",",
                "\"seed\":\"0000000000000001\",",
                "\"steps\":10}"
            )
        );
    }
}
