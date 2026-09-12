//! Validates every suite record against the machine-readable schema fixture
//! and checks that the roadmap workload checklist is covered.

use hft_bench::{SuiteConfig, run_suite};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
enum Json {
    Str(String),
    Num(f64),
    Bool(bool),
    Arr(Vec<Json>),
    Obj(HashMap<String, Json>),
}

struct Parser<'a> {
    bytes: &'a [u8],
}

impl<'a> Parser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes(),
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.bytes.first(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.bytes = &self.bytes[1..];
        }
    }

    fn parse(&mut self) -> Json {
        self.skip_ws();
        match self.bytes.first() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => Json::Str(self.parse_string()),
            Some(_) => self.parse_number_or_bool(),
            None => panic!("unexpected end of JSON"),
        }
    }

    fn parse_object(&mut self) -> Json {
        self.bytes = &self.bytes[1..];
        let mut map = HashMap::new();
        self.skip_ws();
        if self.bytes.first() == Some(&b'}') {
            self.bytes = &self.bytes[1..];
            return Json::Obj(map);
        }
        loop {
            self.skip_ws();
            let key = self.parse_string();
            self.skip_ws();
            assert_eq!(self.bytes.first(), Some(&b':'), "expected colon");
            self.bytes = &self.bytes[1..];
            let value = self.parse();
            map.insert(key, value);
            self.skip_ws();
            match self.bytes.first() {
                Some(b',') => self.bytes = &self.bytes[1..],
                Some(b'}') => {
                    self.bytes = &self.bytes[1..];
                    return Json::Obj(map);
                }
                other => panic!("expected , or }}, found {other:?}"),
            }
        }
    }

    fn parse_array(&mut self) -> Json {
        self.bytes = &self.bytes[1..];
        let mut items = Vec::new();
        self.skip_ws();
        if self.bytes.first() == Some(&b']') {
            self.bytes = &self.bytes[1..];
            return Json::Arr(items);
        }
        loop {
            items.push(self.parse());
            self.skip_ws();
            match self.bytes.first() {
                Some(b',') => self.bytes = &self.bytes[1..],
                Some(b']') => {
                    self.bytes = &self.bytes[1..];
                    return Json::Arr(items);
                }
                other => panic!("expected , or ], found {other:?}"),
            }
        }
    }

    fn parse_string(&mut self) -> String {
        assert_eq!(self.bytes.first(), Some(&b'"'), "expected string");
        self.bytes = &self.bytes[1..];
        let end = self
            .bytes
            .iter()
            .position(|byte| *byte == b'"')
            .expect("closed string");
        let text = String::from_utf8(self.bytes[..end].to_vec()).expect("utf8");
        self.bytes = &self.bytes[end + 1..];
        text
    }

    fn parse_number_or_bool(&mut self) -> Json {
        let end = self
            .bytes
            .iter()
            .position(|byte| matches!(byte, b',' | b'}' | b']' | b' ' | b'\n'))
            .unwrap_or(self.bytes.len());
        let token = std::str::from_utf8(&self.bytes[..end]).expect("token utf8");
        self.bytes = &self.bytes[end..];
        if token == "true" {
            return Json::Bool(true);
        }
        if token == "false" {
            return Json::Bool(false);
        }
        Json::Num(token.parse().expect("numeric token"))
    }
}

fn parse_json(text: &str) -> Json {
    let mut parser = Parser::new(text);
    let value = parser.parse();
    parser.skip_ws();
    assert!(parser.bytes.is_empty(), "trailing JSON input");
    value
}

fn strings(value: &Json) -> Vec<String> {
    match value {
        Json::Arr(items) => items
            .iter()
            .map(|item| match item {
                Json::Str(text) => text.clone(),
                other => panic!("expected string array, found {other:?}"),
            })
            .collect(),
        other => panic!("expected array, found {other:?}"),
    }
}

fn number(record: &HashMap<String, Json>, key: &str) -> f64 {
    match record.get(key) {
        Some(Json::Num(value)) => *value,
        other => panic!("{key} must be a number, found {other:?}"),
    }
}

/// Exact unsigned field read; benchmark counters are non-negative integers.
fn count(record: &HashMap<String, Json>, key: &str) -> u64 {
    let value = number(record, key);
    assert!(value >= 0.0 && value.fract() == 0.0, "{key} not an integer");
    format!("{value:.0}").parse().expect("integer field")
}

struct Schema {
    boundaries: HashSet<String>,
    components: HashSet<String>,
    required_strings: Vec<String>,
    required_numbers: Vec<String>,
    param_names: HashSet<String>,
}

impl Schema {
    fn load() -> Self {
        let schema_text = include_str!("fixtures/bench_results.schema.json");
        let schema = match parse_json(schema_text) {
            Json::Obj(map) => map,
            other => panic!("schema root must be an object, found {other:?}"),
        };
        assert_eq!(
            schema.get("schema"),
            Some(&Json::Str("hft-bench-results/1".to_string()))
        );
        Self {
            boundaries: strings(schema.get("boundaries").unwrap())
                .into_iter()
                .collect(),
            components: strings(schema.get("components").unwrap())
                .into_iter()
                .collect(),
            required_strings: strings(schema.get("required_strings").unwrap()),
            required_numbers: strings(schema.get("required_numbers").unwrap()),
            param_names: strings(schema.get("param_names").unwrap())
                .into_iter()
                .collect(),
        }
    }
}

/// Validates one record line and returns its (component, scenario) identity.
#[allow(clippy::too_many_lines)]
fn validate_record(line: &str, schema: &Schema) -> (String, String) {
    let Json::Obj(record) = parse_json(line) else {
        panic!("record line is not an object: {line}");
    };
    for key in &schema.required_strings {
        assert!(
            matches!(record.get(key), Some(Json::Str(_))),
            "{key} missing or not a string: {line}"
        );
    }
    for key in &schema.required_numbers {
        assert!(record.contains_key(key), "{key} missing: {line}");
        number(&record, key);
    }
    let boundary = match record.get("boundary") {
        Some(Json::Str(text)) => text.clone(),
        other => panic!("boundary invalid: {other:?}"),
    };
    assert!(
        schema.boundaries.contains(&boundary),
        "unknown boundary {boundary}"
    );
    let component = match record.get("component") {
        Some(Json::Str(text)) => text.clone(),
        other => panic!("component invalid: {other:?}"),
    };
    assert!(
        schema.components.contains(&component),
        "unknown component {component}"
    );
    let scenario = match record.get("scenario") {
        Some(Json::Str(text)) => text.clone(),
        other => panic!("scenario invalid: {other:?}"),
    };

    // Checksum: 16 lowercase hex characters.
    let checksum = match record.get("checksum") {
        Some(Json::Str(text)) => text.clone(),
        other => panic!("checksum invalid: {other:?}"),
    };
    assert_eq!(checksum.len(), 16, "checksum width: {line}");
    assert!(
        checksum
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "checksum hex: {checksum}"
    );

    // Params: known names.
    match record.get("params") {
        Some(Json::Obj(params)) => {
            for name in params.keys() {
                assert!(
                    schema.param_names.contains(name),
                    "unknown param {name}: {line}"
                );
            }
        }
        other => panic!("params must be an object, found {other:?}"),
    }

    // Recovery runs off the hot path and reports its allocation cost.
    let (p50, p90, p99, p99_9, max) = (
        number(&record, "p50_ns"),
        number(&record, "p90_ns"),
        number(&record, "p99_ns"),
        number(&record, "p99_9_ns"),
        number(&record, "max_ns"),
    );
    assert!(count(&record, "samples") >= 1, "empty samples: {line}");
    assert!(
        p50 <= p90 && p90 <= p99 && p99 <= p99_9 && p99_9 <= max,
        "percentile order: {line}"
    );
    if component != "recovery" {
        assert_eq!(count(&record, "allocations"), 0, "allocations: {line}");
        assert_eq!(count(&record, "deallocations"), 0, "deallocations: {line}");
    }

    (component, scenario)
}

#[test]
fn suite_records_match_the_schema_fixture() {
    let schema = Schema::load();
    if hft_spsc::IS_LOOM_BUILD {
        // A Loom-enabled dependency build cannot execute the timed suite.
        eprintln!("skipping schema validation under loom build");
        return;
    }
    let lines = run_suite(SuiteConfig::reduced());
    assert!(lines.len() >= 40, "suite emitted {}", lines.len());

    let mut covered: HashSet<(String, String)> = HashSet::new();
    for line in &lines {
        covered.insert(validate_record(line, &schema));
    }

    // Roadmap v0.8.0 workload coverage.
    let expected = [
        ("gateway", "pair_rest_fill"),
        ("gateway", "mixed_seeded"),
        ("queue", "capacity_smoke"),
        ("parser", "parse_frames"),
        ("spsc", "push_pop_walk"),
        ("book", "cancel_sweep"),
        ("book", "head_cancel"),
        ("book", "middle_cancel"),
        ("book", "tail_cancel"),
        ("book", "head_fill"),
        ("book", "submit_cross"),
        ("book", "discovery"),
        ("book", "level_create"),
        ("book", "non_crossing"),
        ("book", "single_fill"),
        ("book", "multi_fill"),
        ("book", "report_full"),
        ("book", "deep_rejection"),
        ("book", "deep_book"),
        ("book", "top_level_pair"),
        ("book", "submit_top_cancel_top"),
        ("router", "lookup_hit"),
        ("router", "lookup_mixed"),
        ("router", "reverse_lookup"),
        ("engine", "journaled_command"),
        ("book", "replace_reduce"),
        ("book", "replace_increase"),
        ("book", "replace_reprice"),
        ("book", "replace_reject_unknown"),
        ("risk", "risk_check"),
        ("risk", "reservation_lookup"),
        ("risk", "fill"),
        ("risk", "cancel"),
        ("risk", "settle"),
        ("risk", "reject"),
        ("risk", "account_lookup"),
        ("journal", "controlled_journal_enqueue"),
        ("journal", "controlled_journal_persistence_memory"),
        ("recovery", "canonical_snapshot_encode"),
        ("recovery", "verified_snapshot_restore"),
        ("recovery", "snapshot_tail_replay"),
        ("events", "admitted_push_pop"),
        ("events", "full_queue_backpressure"),
        ("router", "route_command"),
        ("router", "route_shard_event"),
        ("router", "full_command_queue"),
    ];
    for (component_name, scenario_name) in expected {
        assert!(
            covered.contains(&(component_name.to_string(), scenario_name.to_string())),
            "missing workload {component_name}/{scenario_name}"
        );
    }
}
