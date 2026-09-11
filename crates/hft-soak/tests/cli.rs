use hft_soak::{Profile, ResultError, RunConfig, RunStatus, Seed, run, run_verified};
use std::process::{Command, Output};

fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hft-soak"))
        .args(args)
        .output()
        .expect("start hft-soak")
}

#[test]
fn cli_runs_scenarios_and_emits_identical_results_for_the_same_seed() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let args = ["--seed", "0000000000000001", "--steps", "2"];
    let first = cli(&args);
    assert!(first.status.success(), "{:?}", first.stderr);
    assert!(first.stderr.is_empty());
    let second = cli(&args);
    assert!(second.status.success(), "{:?}", second.stderr);
    assert_eq!(first.stdout, second.stdout);
    let line = String::from_utf8(first.stdout).expect("UTF-8 JSON");
    assert_eq!(line.lines().count(), 1);
    assert!(line.starts_with("{\"schema\":\"hft-soak-results/1\",\"profile\":\"smoke\""));
    assert!(line.contains("\"steps\":2,\"completed_steps\":2,\"status\":\"passed\""));
    assert!(line.contains("\"routed\":{\"steps\":2,"));
    assert!(line.contains("\"session\":{\"steps\":1,\"accepted_commands\":3,"));
    assert!(line.contains("\"recovery\":{\"steps\":2,\"commands\":2,"));
    assert!(line.contains("\"journal\":{\"saturation_refusals\":1,\"retry_successes\":1,"));
    assert!(line.contains("\"capacity\":{\"price_level_order_refusals\":1,"));
    assert!(
        line.trim_end()
            .ends_with("\"peak_rss_bytes\":null,\"failure\":null}")
    );
}

#[test]
fn runner_scales_work_and_checks_determinism_without_resource_measurements() {
    if hft_spsc::IS_LOOM_BUILD {
        return;
    }
    let config = RunConfig::new(Profile::Smoke, Seed(1), 65).expect("valid config");
    let result = run_verified(config).expect("verified scenarios");
    assert_eq!(result.completed_steps, 65);
    assert_eq!(result.scenarios.routed.steps, 65);
    assert_eq!(result.scenarios.recovery.commands, 65);
    assert_eq!(result.scenarios.recovery.checkpoints, 2);
    assert_eq!(result.scenarios.session.steps, 2);
    assert_eq!(result.scenarios.session.reconnects, 2);
    assert_eq!(result.scenarios.journal.hard_write_failures, 1);
    assert_eq!(result.scenarios.capacity.event_queue_retries, 1);
    assert_eq!(result.peak_rss_bytes, None);

    let mut measured = result.clone();
    measured.peak_rss_bytes = Some(4_096);
    assert!(result.deterministic_eq(&measured));
    measured.scenarios.routed.events += 1;
    assert!(!result.deterministic_eq(&measured));

    let mut incomplete = result.clone();
    incomplete.completed_steps -= 1;
    assert_eq!(
        incomplete.validate(),
        Err(ResultError::PassedBeforeCompletion)
    );
    incomplete.status = RunStatus::Failed;
    assert_eq!(
        incomplete.validate(),
        Err(ResultError::FailedWithoutFailure)
    );
    incomplete.failure = Some("stopped".to_owned());
    assert_eq!(incomplete.validate(), Ok(()));
    let mut wrong_count = result;
    wrong_count.scenarios.recovery.commands -= 1;
    assert_eq!(
        wrong_count.validate(),
        Err(ResultError::ScenarioStepsMismatch)
    );
}

#[test]
fn runner_rejects_invalid_public_configs_before_starting_scenarios() {
    for steps in [0, u64::MAX] {
        let config = RunConfig {
            profile: Profile::Smoke,
            seed: Seed(0x1234),
            steps,
        };
        let error = run(config).expect_err("invalid step bound");
        assert_eq!(error.config, config);
        assert_eq!(error.phase, "configuration");
        assert!(error.replay_command().contains("--seed 0000000000001234"));
        let line = error.to_json_line();
        assert!(line.contains("\"phase\":\"configuration\""));
        assert!(line.contains("\"seed\":\"0000000000001234\""));
        assert!(line.contains("\"replay_command\":\"cargo run --release -p hft-soak"));
    }
}

#[test]
fn cli_rejects_invalid_or_conflicting_inputs() {
    for args in [
        vec!["--steps", "0"],
        vec!["--steps", "18446744073709551615"],
        vec!["--seed", "1234"],
        vec!["--unknown"],
        vec!["--seed", "0000000000000001", "--seed-file", "missing.txt"],
    ] {
        let result = cli(&args);
        assert!(!result.status.success(), "unexpected success for {args:?}");
        assert!(!result.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&result.stdout).contains("\"status\":\"passed\""));
    }
}
