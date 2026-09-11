//! Run the reduced suite with the binary's counting allocator.

use std::process::Command;

#[test]
fn reduced_suite_checks_hot_path_and_recovery_allocations() {
    if hft_spsc::IS_LOOM_BUILD {
        eprintln!("skipping: loom build cannot execute the timed suite");
        return;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_hft-bench"))
        .arg("--reduced")
        .output()
        .expect("run benchmark binary");
    assert!(
        output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 benchmark output");
    let lines: Vec<_> = stdout.lines().collect();
    assert!(lines.len() > 40, "suite emitted {}", lines.len());
    let mut recovery_cells = 0;
    for line in lines {
        assert!(
            line.starts_with("{\"schema\":\"hft-bench-results/1\","),
            "{line}"
        );
        let allocations = numeric_field(line, "allocations");
        let deallocations = numeric_field(line, "deallocations");
        if line.contains("\"component\":\"recovery\"") {
            recovery_cells += 1;
            assert!(allocations > 0, "allocator was not observed: {line}");
            assert!(deallocations > 0, "deallocator was not observed: {line}");
        } else {
            assert_eq!(allocations, 0, "{line}");
            assert_eq!(deallocations, 0, "{line}");
        }
    }
    assert_eq!(recovery_cells, 3);
}

#[test]
fn invalid_options_fail_without_running_the_suite() {
    for args in [vec!["--unknown"], vec!["--reduced", "--reduced"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_hft-bench"))
            .args(args)
            .output()
            .expect("run benchmark binary");
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr)
                .expect("UTF-8 usage output")
                .trim(),
            "usage: hft-bench [--reduced]"
        );
    }
}

fn numeric_field(line: &str, name: &str) -> u64 {
    let key = format!("\"{name}\":");
    let (_, value) = line.split_once(&key).expect("numeric field exists");
    value
        .split([',', '}'])
        .next()
        .expect("numeric field value exists")
        .parse()
        .expect("numeric field is u64")
}
