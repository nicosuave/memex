#![cfg(unix)]
//! End-to-end dreaming through the CLI, with a shell script standing in for the model.

use memex::analytics::{analytics_path, backfill_from_index};
use memex::config::Paths;
use memex::index::SearchIndex;
use memex::types::{Record, RecordLinks, SourceKind};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const BUN_TRANSCRIPT: &str = "/tmp/memex-dream-fixture/bun.jsonl";
const CI_TRANSCRIPT: &str = "/tmp/memex-dream-fixture/ci.jsonl";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn record(
    id: u64,
    source: SourceKind,
    session: &str,
    path: &str,
    role: &str,
    ts: u64,
    text: &str,
) -> Record {
    Record {
        source,
        doc_id: id,
        ts,
        project: "shop".into(),
        session_id: session.into(),
        turn_id: id as u32,
        role: role.into(),
        text: text.into(),
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: RecordLinks {
            event_id: Some(format!("event-{id}")),
            ..Default::default()
        },
        source_path: path.into(),
    }
}

struct Fixture {
    root: tempfile::TempDir,
    /// Where the fake executor saves the prompt it was given.
    prompt: PathBuf,
    /// What the fake executor answers.
    answer: PathBuf,
}

impl Fixture {
    /// Two sessions from two agents. The Claude session is the newer one, so its turns are
    /// offered first: `r1` is the correction, `r2` the reply, `r3` the Codex statement.
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
        let now = now_ms();
        let records = [
            record(
                1,
                SourceKind::Codex,
                "ci-session",
                CI_TRANSCRIPT,
                "user",
                now - 600_000,
                "Remember that CI for the shop runs on Buildkite.",
            ),
            record(
                2,
                SourceKind::Claude,
                "bun-session",
                BUN_TRANSCRIPT,
                "user",
                now - 120_000,
                "No, always use bun in this repo, never npm. Token ghp_abcdefghijklmnopqrstuvwxyz0123456789.",
            ),
            record(
                3,
                SourceKind::Claude,
                "bun-session",
                BUN_TRANSCRIPT,
                "tool_result",
                now - 110_000,
                "npm WARN deprecated tool output that a dream never reads",
            ),
            record(
                4,
                SourceKind::Claude,
                "bun-session",
                BUN_TRANSCRIPT,
                "assistant",
                now - 100_000,
                "Understood, switching the install step to bun.",
            ),
        ];
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        for record in &records {
            index.add_record(&mut writer, record).unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        backfill_from_index(analytics_path(&paths.state), &index).unwrap();

        let prompt = root.path().join("executor-prompt.txt");
        let answer = root.path().join("executor-answer.json");
        let script = root.path().join("executor.sh");
        std::fs::write(
            &script,
            format!("cat > '{}'\ncat '{}'\n", prompt.display(), answer.display()),
        )
        .unwrap();
        std::fs::write(
            root.path().join("config.toml"),
            format!(
                "auto_index_on_search = false\n\n[dream]\nenabled = true\ncommand = [\"sh\", \"{}\"]\n",
                script.display()
            ),
        )
        .unwrap();
        Self {
            root,
            prompt,
            answer,
        }
    }

    fn answer(&self, json: &str) {
        std::fs::write(&self.answer, json).unwrap();
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_memex"))
            .args(args)
            .arg("--root")
            .arg(self.root.path())
            .output()
            .unwrap()
    }

    fn stdout(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    fn json(&self, args: &[&str]) -> Value {
        let stdout = self.stdout(args);
        serde_json::from_str(&stdout).unwrap_or_else(|error| panic!("{args:?}: {error}: {stdout}"))
    }

    fn dreams_dir(&self) -> PathBuf {
        self.root.path().join("dreams")
    }
}

fn delete_transcript(root: &Path, source_path: &str) {
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    index.delete_by_source_path(&mut writer, source_path);
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
}

const ANSWER: &str = r#"Here is the consolidation.
```json
{"ops":[
  {"op":"add","kind":"correction","project":"shop",
   "text":"Use bun, never npm, in the shop repository.",
   "evidence":[{"ref":"r1","quote":"always use bun in this repo"}]},
  {"op":"add","kind":"project_fact","project":"shop",
   "text":"Shop CI runs on Buildkite.",
   "evidence":["r3"]},
  {"op":"add","kind":"lesson","text":"A dream that cites nothing memex offered.",
   "evidence":["r41"]}
]}
```"#;

#[test]
fn a_dream_publishes_cited_dreams_that_search_and_lint_can_follow() {
    let fixture = Fixture::new();
    fixture.answer(ANSWER);

    // A dry run judges and reports but leaves the store untouched.
    let dry = fixture.json(&["dream", "--dry-run", "--format", "json"]);
    assert_eq!(dry["counts"]["added"], 2);
    assert_eq!(dry["counts"]["rejected"], 1);
    assert_eq!(dry["sessions"], 2);
    assert_eq!(dry["records"], 3);
    assert!(dry.get("generation").is_none());
    assert!(dry["diff"].as_str().unwrap().contains("Use bun, never npm"));
    assert!(!fixture.dreams_dir().join("CURRENT").exists());

    let prompt = std::fs::read_to_string(&fixture.prompt).unwrap();
    assert!(prompt.starts_with("MEMEX-DREAM-PROMPT v1"));
    assert!(prompt.contains("[r1]") && prompt.contains("always use bun in this repo"));
    assert!(prompt.contains("CI for the shop runs on Buildkite"));
    assert!(!prompt.contains("tool output that a dream never reads"));
    assert!(!prompt.contains("ghp_abcdefghijklmnopqrstuvwxyz0123456789"));

    let published = fixture.json(&["dream", "--format", "json"]);
    let first_generation = published["generation"].as_str().unwrap().to_string();
    assert_eq!(published["active_dreams"], 2);
    assert_eq!(published["rejected"][0]["op"], "add");
    assert_eq!(
        std::fs::read_to_string(fixture.dreams_dir().join("CURRENT"))
            .unwrap()
            .trim(),
        first_generation
    );

    let shown = fixture.stdout(&["dream", "show"]);
    assert!(shown.contains("## shop"));
    assert!(shown.contains("Use bun, never npm, in the shop repository."));
    assert!(shown.contains("> always use bun in this repo, never npm."));
    assert!(!shown.contains("ghp_abcdefghijklmnopqrstuvwxyz0123456789"));
    assert!(fixture.stdout(&["dream", "diff"]).contains("+ added `drm_"));

    // Dreams are a search corpus of their own, and each hit leads back to a real record.
    let hits = fixture.stdout(&["search", "bun", "--content", "dreams"]);
    let hit: Value = serde_json::from_str(hits.lines().next().unwrap()).unwrap();
    assert_eq!(hits.lines().count(), 1);
    assert_eq!(hit["kind"], "dream");
    assert_eq!(hit["dream_kind"], "correction");
    assert_eq!(hit["evidence"], "supported");
    let record_id = hit["citations"][0]["record_id"].as_str().unwrap();
    let source = fixture.json(&["show", "--record-id", record_id, "--machine", "local"]);
    assert!(
        source["record"]["text"]
            .as_str()
            .unwrap()
            .starts_with("No, always use bun")
    );
    let semantic = fixture.run(&["search", "bun", "--content", "dreams", "--mode", "semantic"]);
    assert!(!semantic.status.success());

    // Ordinary recall over everything returns the dream beside the record it rests on,
    // without the caller knowing a dream store exists.
    let kinds = |args: &[&str]| -> Vec<String> {
        fixture
            .stdout(args)
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["kind"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    };
    let recall = kinds(&["search", "buildkite", "--content", "all"]);
    assert!(recall.contains(&"dream".to_string()), "{recall:?}");
    assert!(recall.contains(&"conversation".to_string()), "{recall:?}");
    // A dream cites both agents' sessions only if it rests on both; each filter keeps its own.
    assert!(
        kinds(&[
            "search",
            "buildkite",
            "--content",
            "all",
            "--source",
            "codex"
        ])
        .contains(&"dream".to_string())
    );
    assert!(
        !kinds(&[
            "search",
            "buildkite",
            "--content",
            "all",
            "--source",
            "claude"
        ])
        .contains(&"dream".to_string())
    );
    // A filter only a conversation record can satisfy leaves dreams out.
    assert_eq!(
        kinds(&["search", "buildkite", "--content", "all", "--role", "user"]),
        ["conversation"]
    );
    // Conversation search without a content type is unchanged.
    let plain = fixture.stdout(&["search", "buildkite"]);
    assert!(!plain.contains("dream_id"));

    // Nothing new has happened, so the next dream run has nothing to judge.
    std::fs::remove_file(&fixture.prompt).unwrap();
    let idle = fixture.json(&["dream", "--format", "json"]);
    assert!(
        idle["skipped"]
            .as_str()
            .unwrap()
            .contains("no new sessions")
    );
    assert!(idle.get("generation").is_none());
    assert!(!fixture.prompt.exists());

    // The Claude transcript is deleted, as Claude Code does after its retention period.
    delete_transcript(fixture.root.path(), BUN_TRANSCRIPT);
    let preview = fixture.json(&["dream", "lint", "--dry-run", "--format", "json"]);
    assert_eq!(preview["evidence_lost"], 1);
    assert!(preview.get("generation").is_none());
    let lint = fixture.json(&["dream", "lint", "--format", "json"]);
    assert_eq!(lint["evidence_lost"], 1);
    assert_eq!(lint["resolved"], 1);
    assert_eq!(lint["dreams_without_evidence"], 1);
    let lint_generation = lint["generation"].as_str().unwrap();
    assert_ne!(lint_generation, first_generation);

    let dream_id = hit["dream_id"].as_str().unwrap();
    let dream = fixture.json(&["dream", "show", dream_id]);
    assert_eq!(dream["status"], "active");
    assert_eq!(dream["evidence"], "lost");
    assert_eq!(dream["citations"][0]["state"], "evidence_lost");
    assert_eq!(
        dream["citations"][0]["excerpt"],
        "always use bun in this repo, never npm. Token [redacted:github-token]."
    );
    // The earlier generation still says what it said when it was published.
    let before = fixture.json(&["dream", "show", dream_id, "--generation", &first_generation]);
    assert_eq!(before["citations"][0]["state"], "resolved");

    let status = fixture.json(&["dream", "status", "--format", "json"]);
    assert_eq!(status["current"], lint_generation);
    assert_eq!(status["generations"], 2);
    assert_eq!(status["dreams"]["active"], 2);
    assert_eq!(status["dreams"]["lost"], 1);
    assert_eq!(status["gate"]["due"], false);
}

#[test]
fn a_failing_executor_publishes_nothing_and_is_remembered() {
    let fixture = Fixture::new();
    fixture.answer("I could not decide.");
    let failed = fixture.run(&["dream"]);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("JSON object"));
    assert!(!fixture.dreams_dir().join("CURRENT").exists());
    let status = fixture.json(&["dream", "status", "--format", "json"]);
    assert!(
        status["last_error"]
            .as_str()
            .unwrap()
            .contains("JSON object")
    );
    assert!(
        status["gate"]["reason"]
            .as_str()
            .unwrap()
            .contains("did not publish")
    );
}

#[test]
fn print_prompt_shows_the_input_without_running_the_executor() {
    let fixture = Fixture::new();
    let prompt = fixture.stdout(&["dream", "--print-prompt", "--source", "codex"]);
    assert!(prompt.starts_with("MEMEX-DREAM-PROMPT v1"));
    assert!(prompt.contains("CI for the shop runs on Buildkite"));
    assert!(!prompt.contains("always use bun"));
    assert!(!fixture.prompt.exists());
    assert!(!fixture.dreams_dir().exists());
}

#[test]
fn the_schedule_gate_opens_on_enough_new_sessions_and_a_quiet_period() {
    use memex::config::UserConfig;
    use memex::dream::schedule::evaluate_gate;

    let fixture = Fixture::new();
    let paths = Paths::new(Some(fixture.root.path().to_path_buf())).unwrap();
    let mut config = UserConfig::load(&paths).unwrap();
    let now = now_ms();
    const HOUR_MS: u64 = 3_600_000;

    // Two sessions are fewer than the default threshold of five.
    let too_few = evaluate_gate(&paths, &config, now + HOUR_MS);
    assert!(!too_few.due);
    assert_eq!(too_few.new_sessions, 2);

    config.dream.min_new_sessions = Some(2);
    // The newest session was active under two minutes ago.
    let busy = evaluate_gate(&paths, &config, now);
    assert!(!busy.due, "{}", busy.reason);
    assert!(busy.reason.contains("quiet"));
    let quiet = evaluate_gate(&paths, &config, now + HOUR_MS);
    assert!(quiet.due, "{}", quiet.reason);

    // After a dream, the interval and the absence of new sessions keep it shut.
    fixture.answer(ANSWER);
    fixture.stdout(&["dream"]);
    let just_dreamed = evaluate_gate(&paths, &config, now + HOUR_MS);
    assert!(!just_dreamed.due);
    assert!(just_dreamed.reason.contains("minimum interval"));
    let next_day = evaluate_gate(&paths, &config, now + 30 * HOUR_MS);
    assert!(!next_day.due);
    assert_eq!(next_day.new_sessions, 0);
}

#[test]
fn a_project_scope_keeps_its_own_dreams_and_those_that_hold_everywhere() {
    let fixture = Fixture::new();
    fixture.answer(
        r#"{"ops":[
          {"op":"add","kind":"project_fact","project":"shop",
           "text":"Shop installs run through bun.","evidence":["r1"]},
          {"op":"add","kind":"preference","project":null,
           "text":"Prefers bun over npm in every repository.","evidence":["r1"]}
        ]}"#,
    );
    fixture.stdout(&["dream"]);
    let dreams = |args: &[&str]| -> Vec<String> {
        let mut texts = fixture
            .stdout(args)
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|hit| hit["kind"] == "dream")
            .map(|hit| hit["snippet"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        texts.sort();
        texts
    };
    let everywhere = "Prefers bun over npm in every repository.";
    let shop = "Shop installs run through bun.";
    assert_eq!(
        dreams(&["search", "bun", "--content", "dreams"]),
        [everywhere, shop]
    );
    assert_eq!(
        dreams(&["search", "bun", "--content", "dreams", "--project", "shop"]),
        [everywhere, shop]
    );
    assert_eq!(
        dreams(&["search", "bun", "--content", "dreams", "--project", "blog"]),
        [everywhere]
    );
    // The same scoping applies when dreams come back with everything else.
    assert_eq!(
        dreams(&["search", "bun", "--content", "all", "--project", "blog"]),
        [everywhere]
    );
}

#[test]
fn nothing_runs_or_surfaces_until_dreaming_is_enabled() {
    let fixture = Fixture::new();
    fixture.answer(ANSWER);
    let config = fixture.root.path().join("config.toml");
    let enabled = std::fs::read_to_string(&config).unwrap();
    let disabled = enabled.replace("enabled = true\n", "");
    assert_ne!(enabled, disabled);
    let refused = |args: &[&str]| {
        let output = fixture.run(args);
        assert!(!output.status.success(), "{args:?} should be refused");
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        assert!(stderr.contains("enabled = true"), "{args:?}: {stderr}");
    };

    // Off is the default: no executor is run and nothing is written, dry run or not.
    std::fs::write(&config, &disabled).unwrap();
    refused(&["dream"]);
    refused(&["dream", "--dry-run"]);
    assert!(!fixture.prompt.exists());
    assert!(!fixture.dreams_dir().exists());
    // Seeing what a run would send needs no opt-in, because it sends nothing.
    assert!(
        fixture
            .stdout(&["dream", "--print-prompt"])
            .starts_with("MEMEX-DREAM-PROMPT v1")
    );
    let status = fixture.json(&["dream", "status", "--format", "json"]);
    assert_eq!(status["enabled"], false);
    assert_eq!(status["gate"]["due"], false);

    // Dreams written while it was on stop surfacing once it is turned off again.
    std::fs::write(&config, &enabled).unwrap();
    fixture.stdout(&["dream"]);
    assert!(
        fixture
            .stdout(&["search", "buildkite", "--content", "all"])
            .contains("\"kind\":\"dream\"")
    );
    std::fs::write(&config, &disabled).unwrap();
    let recall = fixture.stdout(&["search", "buildkite", "--content", "all"]);
    assert!(recall.contains("\"kind\":\"conversation\""));
    assert!(!recall.contains("\"kind\":\"dream\""));
    refused(&["search", "buildkite", "--content", "dreams"]);
    // The store itself is left alone and stays readable.
    assert!(fixture.stdout(&["dream", "show"]).contains("Buildkite"));
}
