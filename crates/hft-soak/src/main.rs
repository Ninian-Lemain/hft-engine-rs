#![forbid(unsafe_code)]

use hft_soak::{
    CliOptions, ConfigError, RETAINED_SEEDS_V1, RetainedSeedError, RunConfig, Seed, SoakError,
    run_verified,
};
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

const USAGE: &str = "usage: hft-soak [--profile smoke|nightly|qualification] [--seed HEX | --seed-file PATH] [--steps COUNT]";

fn main() {
    if let Err(error) = execute() {
        eprintln!("hft-soak: {error}");
        if let AppError::Soak(source) = error {
            println!("{}", source.to_json_line());
            std::process::exit(1);
        }
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
}

fn execute() -> Result<(), AppError> {
    // Debug scenario fixtures exceed the default Windows main-thread stack.
    std::thread::Builder::new()
        .name("hft-soak".to_owned())
        .stack_size(4 * 1024 * 1024)
        .spawn(|| run(std::env::args_os().skip(1)))
        .map_err(AppError::WorkerSpawn)?
        .join()
        .map_err(|_| AppError::WorkerPanicked)?
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
        let result = run_verified(config).map_err(AppError::Soak)?;
        let line = result.to_json_line().map_err(|error| {
            AppError::Soak(SoakError {
                config,
                phase: "result serialization",
                message: error.to_string(),
            })
        })?;
        println!("{line}");
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
    WorkerSpawn(std::io::Error),
    WorkerPanicked,
    NonUnicodeArgument,
    Cli(hft_soak::CliError),
    Configuration(ConfigError),
    Soak(SoakError),
    ReadSeedFile {
        path: PathBuf,
        source: std::io::Error,
    },
    RetainedSeeds(RetainedSeedError),
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerSpawn(source) => write!(formatter, "cannot start soak worker: {source}"),
            Self::WorkerPanicked => formatter.write_str("soak worker panicked"),
            Self::NonUnicodeArgument => formatter.write_str("argument is not valid Unicode"),
            Self::Cli(source) => source.fmt(formatter),
            Self::Configuration(source) => source.fmt(formatter),
            Self::Soak(source) => source.fmt(formatter),
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
            Self::Soak(source) => Some(source),
            Self::WorkerSpawn(source) | Self::ReadSeedFile { source, .. } => Some(source),
            Self::RetainedSeeds(source) => Some(source),
            Self::NonUnicodeArgument | Self::WorkerPanicked => None,
        }
    }
}
