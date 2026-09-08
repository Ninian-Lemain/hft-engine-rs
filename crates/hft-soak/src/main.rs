#![forbid(unsafe_code)]

use hft_soak::{CliOptions, ConfigError, RETAINED_SEEDS_V1, RetainedSeedError, RunConfig, Seed};
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

const USAGE: &str = "usage: hft-soak [--profile smoke|nightly|qualification] [--seed HEX | --seed-file PATH] [--steps COUNT]";

fn main() {
    if let Err(error) = run(std::env::args_os().skip(1)) {
        eprintln!("hft-soak: {error}");
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
}

fn run<I>(args: I) -> Result<(), AppError>
where
    I: IntoIterator<Item = OsString>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.into_string().map_err(|_| AppError::NonUnicodeArgument))
        .collect::<Result<Vec<_>, _>>()?;
    let options = CliOptions::parse(args).map_err(AppError::Cli)?;
    let seeds = load_seeds(&options)?;
    for seed in seeds {
        let config = RunConfig::new(options.profile, seed, options.steps)
            .map_err(AppError::Configuration)?;
        println!("{}", config.to_json_line());
    }
    Ok(())
}

fn load_seeds(options: &CliOptions) -> Result<Vec<Seed>, AppError> {
    if let Some(seed) = options.seed {
        return Ok(vec![seed]);
    }
    let contents = if let Some(path) = &options.seed_file {
        std::fs::read_to_string(path).map_err(|source| AppError::ReadSeedFile {
            path: path.clone(),
            source,
        })?
    } else {
        RETAINED_SEEDS_V1.to_owned()
    };
    hft_soak::parse_retained_seeds(&contents).map_err(AppError::RetainedSeeds)
}

#[derive(Debug)]
enum AppError {
    NonUnicodeArgument,
    Cli(hft_soak::CliError),
    Configuration(ConfigError),
    ReadSeedFile {
        path: PathBuf,
        source: std::io::Error,
    },
    RetainedSeeds(RetainedSeedError),
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonUnicodeArgument => formatter.write_str("argument is not valid Unicode"),
            Self::Cli(source) => source.fmt(formatter),
            Self::Configuration(source) => source.fmt(formatter),
            Self::ReadSeedFile { path, source } => {
                write!(
                    formatter,
                    "cannot read seed file '{}': {source}",
                    path.display()
                )
            }
            Self::RetainedSeeds(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cli(source) => Some(source),
            Self::Configuration(source) => Some(source),
            Self::ReadSeedFile { source, .. } => Some(source),
            Self::RetainedSeeds(source) => Some(source),
            Self::NonUnicodeArgument => None,
        }
    }
}
