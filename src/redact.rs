//! Secret redaction for indexed transcript content.
//!
//! Records are redacted once, where ingest turns parsed records into index documents
//! (`RecordSender::send`), before tool payloads are clipped. Source transcripts are never
//! rewritten: redaction is a property of the derived index, and its fingerprint is folded
//! into each file's parser version so a settings change re-derives already-indexed history.
//!
//! Every built-in pattern names, with a `secret` capture group, only the span to replace, so
//! `GITHUB_TOKEN=[redacted:github-token]` keeps the name. Built-ins are high-precision value
//! formats and named assignments; a value that is plainly a stand-in (`${TOKEN}`, `<key>`) is
//! kept. User patterns replace their `secret` group when they have one, else the whole match.
//!
//! Tool calls are judged from their input (the command run, the files read and written), so
//! output that pattern redaction cannot recognise (`cat .env`, `gh auth token`) is withheld,
//! and the verdict travels to the paired result through [`ToolVerdicts`].

use aho_corasick::AhoCorasick;
use anyhow::{Context, Result};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use once_cell::sync::Lazy;
use regex::{Regex, RegexSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::state::PendingToolCall;
use crate::types::Record;

/// Bump when built-in patterns or tool rules change, so upgrades re-redact indexed history.
pub const REDACTION_VERSION: u32 = 1;

/// Wall-clock budget for redacting one field. `regex` matching is linear, so this only trips
/// on enormous fields; the field is then replaced rather than stored unredacted.
const FIELD_BUDGET: Duration = Duration::from_secs(1);

/// Replacing one span can expose another (a token assigned to a named variable); passes repeat
/// until nothing changes, with this backstop.
const MAX_PASSES: usize = 4;

pub const TIMED_OUT_MARKER: &str = "[redaction timed out]";
const INPUT_WITHHELD_MARKER: &str = "[input withheld: command contains a credential]";
const CONTENT_WITHHELD_MARKER: &str = "[content withheld: sensitive file]";
/// Prefix of every marker redaction writes; matches never re-redact one.
const MARKER_PREFIXES: &[&str] = &[
    "[redacted:",
    "[output withheld",
    "[input withheld",
    "[content withheld",
    "[redaction timed out",
];

/// Why a tool call's output was withheld. Persisted on pending tool calls, so a result read in a
/// later incremental pass than its call still gets the verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WithheldOutput {
    CredentialCommand,
    SensitiveFile,
}

impl WithheldOutput {
    fn marker(self) -> &'static str {
        match self {
            Self::CredentialCommand => "[output withheld: credential command]",
            Self::SensitiveFile => "[output withheld: sensitive file]",
        }
    }
}

struct Builtin {
    kind: &'static str,
    /// ASCII case-insensitive literals, one of which every match contains. A field holding
    /// none of them skips the pattern without running it.
    anchors: &'static [&'static str],
    regex: &'static str,
    /// Named-value patterns only redact values that look like a credential, not prose.
    validate: bool,
}

const fn builtin(
    kind: &'static str,
    anchors: &'static [&'static str],
    regex: &'static str,
) -> Builtin {
    Builtin {
        kind,
        anchors,
        regex,
        validate: false,
    }
}

const fn named(
    kind: &'static str,
    anchors: &'static [&'static str],
    regex: &'static str,
) -> Builtin {
    Builtin {
        kind,
        anchors,
        regex,
        validate: true,
    }
}

// Word boundaries are ASCII (`(?-u:\b)`): a Unicode `\b` defeats the fast engines on the first
// non-ASCII byte, and transcripts are full of them.
static BUILTINS: &[Builtin] = &[
    builtin(
        "aws-access-key-id",
        &["AKIA", "ASIA", "ABIA", "ACCA"],
        r"(?-u:\b)(?<secret>(?:AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16})(?-u:\b)",
    ),
    builtin(
        "aws-secret-access-key",
        &["aws_secret_access_key", "aws_session_token"],
        r#"(?i:aws_(?:secret_access_key|session_token))["']?[ \t]*[:=][ \t]*["']?(?<secret>[A-Za-z0-9/+=]{40,})"#,
    ),
    builtin(
        "anthropic-api-key",
        &["sk-ant-"],
        r"(?<secret>sk-ant-(?:api|admin|oat|ort)[0-9]{2}-[A-Za-z0-9_-]{60,})",
    ),
    builtin(
        "openrouter-api-key",
        &["sk-or-"],
        r"(?<secret>sk-or-v1-[a-f0-9]{64})",
    ),
    builtin(
        "openai-api-key",
        &["sk-"],
        r"(?-u:\b)(?<secret>sk-(?:proj|svcacct|admin)-[A-Za-z0-9_-]{40,}|sk-[A-Za-z0-9]{20}T3BlbkFJ[A-Za-z0-9]{20}|sk-[A-Za-z0-9]{32,})(?-u:\b)",
    ),
    builtin(
        "groq-api-key",
        &["gsk_"],
        r"(?-u:\b)(?<secret>gsk_[A-Za-z0-9]{52})",
    ),
    builtin(
        "xai-api-key",
        &["xai-"],
        r"(?-u:\b)(?<secret>xai-[A-Za-z0-9]{60,})",
    ),
    builtin(
        "perplexity-api-key",
        &["pplx-"],
        r"(?-u:\b)(?<secret>pplx-[A-Za-z0-9]{48})",
    ),
    builtin(
        "google-api-key",
        &["AIza"],
        r"(?<secret>AIza[0-9A-Za-z_-]{35})",
    ),
    builtin(
        "google-oauth-token",
        &["ya29."],
        r"(?-u:\b)(?<secret>ya29\.[0-9A-Za-z_-]{30,})",
    ),
    builtin(
        "huggingface-token",
        &["hf_"],
        r"(?-u:\b)(?<secret>hf_[A-Za-z]{34,})",
    ),
    builtin(
        "github-token",
        &["ghp_", "gho_", "ghu_", "ghs_", "ghr_"],
        r"(?-u:\b)(?<secret>gh[pousr]_[A-Za-z0-9]{36,})",
    ),
    builtin(
        "github-token",
        &["github_pat_"],
        r"(?-u:\b)(?<secret>github_pat_[A-Za-z0-9_]{60,})",
    ),
    builtin(
        "gitlab-token",
        &["glpat-", "glptt-", "glrt-"],
        r"(?-u:\b)(?<secret>glpat-[A-Za-z0-9_-]{20,}(?:\.[0-9a-z]{9})?|glptt-[0-9a-f]{40}|glrt-[A-Za-z0-9_-]{20,})",
    ),
    builtin(
        "slack-token",
        &["xox", "xapp-"],
        r"(?-u:\b)(?<secret>xox[abeoprs]-[0-9A-Za-z-]{10,}|xapp-[0-9]-[A-Z0-9]+-[0-9]+-[a-z0-9]+)",
    ),
    builtin(
        "slack-webhook",
        &["hooks.slack.com"],
        r"(?<secret>https://hooks\.slack\.com/(?:services|workflows|triggers)/[A-Za-z0-9_/]{20,})",
    ),
    builtin(
        "stripe-key",
        &["sk_live_", "sk_test_", "rk_live_", "rk_test_", "whsec_"],
        r"(?-u:\b)(?<secret>(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{24,}|whsec_[A-Za-z0-9]{32,})",
    ),
    builtin(
        "npm-token",
        &["npm_"],
        r"(?-u:\b)(?<secret>npm_[A-Za-z0-9]{36})",
    ),
    builtin(
        "pypi-token",
        &["pypi-AgEI"],
        r"(?<secret>pypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{50,})",
    ),
    builtin(
        "sendgrid-api-key",
        &["SG."],
        r"(?-u:\b)(?<secret>SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43})",
    ),
    builtin(
        "digitalocean-token",
        &["_v1_"],
        r"(?-u:\b)(?<secret>do[opr]_v1_[a-f0-9]{64})",
    ),
    builtin(
        "linear-api-key",
        &["lin_api_"],
        r"(?<secret>lin_api_[A-Za-z0-9]{40})",
    ),
    builtin(
        "doppler-token",
        &["dp."],
        r"(?-u:\b)(?<secret>dp\.(?:pt|st|ct|sa|scim|audit)\.[A-Za-z0-9]{40,44})",
    ),
    builtin(
        "vault-token",
        &["hvs.", "hvb."],
        r"(?-u:\b)(?<secret>hv[sb]\.[A-Za-z0-9_-]{60,})",
    ),
    builtin(
        "age-secret-key",
        &["AGE-SECRET-KEY-"],
        r"(?<secret>AGE-SECRET-KEY-1[QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L]{58})",
    ),
    builtin(
        "1password-token",
        &["ops_eyJ"],
        r"(?<secret>ops_eyJ[A-Za-z0-9+/]{60,}={0,3})",
    ),
    builtin(
        "tailscale-key",
        &["tskey-"],
        r"(?<secret>tskey-[a-z]+-[A-Za-z0-9]{8,}-[A-Za-z0-9]{20,})",
    ),
    builtin(
        "docker-hub-token",
        &["dckr_pat_"],
        r"(?<secret>dckr_pat_[A-Za-z0-9_-]{27,})",
    ),
    builtin(
        "databricks-token",
        &["dapi"],
        r"(?-u:\b)(?<secret>dapi[a-f0-9]{32}(?:-[0-9])?)(?-u:\b)",
    ),
    builtin(
        "supabase-key",
        &["sb_secret_", "sbp_"],
        r"(?-u:\b)(?<secret>sb_secret_[A-Za-z0-9_-]{31,}|sbp_[a-f0-9]{40})",
    ),
    builtin(
        "shopify-token",
        &["shpat_", "shpca_", "shppa_", "shpss_"],
        r"(?<secret>shp(?:at|ca|pa|ss)_[a-fA-F0-9]{32})",
    ),
    builtin(
        "atlassian-token",
        &["ATATT3"],
        r"(?<secret>ATATT3[A-Za-z0-9_=-]{60,})",
    ),
    builtin(
        "sentry-token",
        &["sntryu_", "sntrys_"],
        r"(?<secret>sntryu_[a-f0-9]{64}|sntrys_eyJ[A-Za-z0-9+/=_]{40,})",
    ),
    builtin(
        "grafana-token",
        &["glsa_", "glc_"],
        r"(?-u:\b)(?<secret>glc_[A-Za-z0-9+/]{32,}={0,3}|glsa_[A-Za-z0-9]{32}_[A-Fa-f0-9]{8})",
    ),
    builtin(
        "telegram-bot-token",
        &[":AA"],
        r"(?-u:\b)(?<secret>[0-9]{8,10}:AA[A-Za-z0-9_-]{33})",
    ),
    builtin(
        "discord-webhook",
        &["discord"],
        r"(?<secret>https://(?:ptb\.|canary\.)?discord(?:app)?\.com/api/webhooks/[0-9]+/[A-Za-z0-9_-]{60,})",
    ),
    builtin(
        "azure-storage-key",
        &["AccountKey="],
        r"AccountKey=(?<secret>[A-Za-z0-9+/]{40,}={0,2})",
    ),
    // The body must start like key material, so code that merely names the armour line
    // (`line == "-----BEGIN PRIVATE KEY-----"`) is not swallowed to the end of the field.
    builtin(
        "private-key",
        &["PRIVATE KEY"],
        r"-----BEGIN[ A-Z0-9_-]{0,100}PRIVATE KEY(?: BLOCK)?-----(?<secret>(?:\s|\\[rn])*(?:Proc-Type:[^\n\\]*(?:\s|\\[rn])+DEK-Info:[^\n\\]*(?:\s|\\[rn])+)?[A-Za-z0-9+/=]{16}(?s:.*?))(?:-----END[ A-Z0-9_-]{0,100}PRIVATE KEY(?: BLOCK)?-----|\z)",
    ),
    builtin(
        "jwt",
        &["eyJ"],
        r"(?-u:\b)(?<secret>eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})",
    ),
    builtin(
        "url-password",
        &["://"],
        r#"://[^\s/?#@"'`<>:\\]*:(?<secret>[^\s/?#@"'`<>\\$*{][^\s/?#@"'`<>\\]*)@"#,
    ),
    builtin(
        "connection-string-password",
        &["password=", "passwd=", "pwd="],
        r#"(?i:(?-u:\b)(?:password|passwd|pwd))=(?<secret>[^&;\s"'`$<{\\*][^&;\s"'`\\]*)"#,
    ),
    named(
        "bearer-token",
        &["bearer"],
        r"(?i:(?-u:\b)bearer)[ \t]+(?<secret>[A-Za-z0-9._~+/-]{16,}=*)",
    ),
    named(
        "authorization-header",
        &["authorization"],
        r#"(?i:(?-u:\b)authorization)["']?[ \t]*[:=][ \t]*["']?(?i:basic|token)[ \t]+(?<secret>[A-Za-z0-9._~+/-]{8,}=*)"#,
    ),
    named(
        "api-key-header",
        &["api-key", "auth-token"],
        r#"(?i:(?-u:\b)(?:x-api-key|api-key|x-auth-token))["']?[ \t]*:[ \t]*["']?(?<secret>[A-Za-z0-9._~+/=-]{16,})"#,
    ),
    // `GITHUB_TOKEN=...`, `export DB_PASSWORD='...'`, `"OPENAI_API_KEY": "..."`. A colon only
    // separates a quoted value, so a type annotation (`API_KEY: Optional[str]`) is not a value.
    named(
        "credential-assignment",
        &[
            "TOKEN",
            "SECRET",
            "PASSWORD",
            "PASSWD",
            "API_KEY",
            "APIKEY",
            "ACCESS_KEY",
            "PRIVATE_KEY",
            "AUTH_KEY",
        ],
        r#"(?-u:\b)(?:[A-Z][A-Z0-9_]*)?(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|APIKEY|ACCESS_KEY|PRIVATE_KEY|AUTH_KEY)S?(?-u:\b)\\?["']?[ \t]*(?:(?::=|=)[ \t]*\\?["']?|:[ \t]*\\?["'])(?<secret>[^\s"'`\\,;&|()\[\]]+)"#,
    ),
    // JSON written by tools and agents' own credential stores (`auth.json`).
    named(
        "credential-field",
        &["token", "secret", "password", "passwd", "api_key", "apikey"],
        r#"\\?"(?:access_token|refresh_token|id_token|auth_token|bearer_token|session_token|client_secret|secret_key|api_key|apiKey|password|passwd|secret|token)\\?"[ \t]*:[ \t]*\\?"(?<secret>[^"\\\s]{8,})"#,
    ),
];

struct CompiledBuiltins {
    anchors: AhoCorasick,
    /// For each anchor, the builtins that need it.
    anchor_patterns: Vec<Vec<usize>>,
    regexes: Vec<Regex>,
}

static COMPILED: Lazy<CompiledBuiltins> = Lazy::new(|| {
    let mut anchor_index: HashMap<String, usize> = HashMap::new();
    let mut anchors: Vec<String> = Vec::new();
    let mut anchor_patterns: Vec<Vec<usize>> = Vec::new();
    for (pattern, builtin) in BUILTINS.iter().enumerate() {
        for anchor in builtin.anchors {
            let key = anchor.to_ascii_lowercase();
            let slot = *anchor_index.entry(key.clone()).or_insert_with(|| {
                anchors.push(key);
                anchor_patterns.push(Vec::new());
                anchors.len() - 1
            });
            anchor_patterns[slot].push(pattern);
        }
    }
    CompiledBuiltins {
        anchors: AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .build(&anchors)
            .expect("redaction anchors compile"),
        anchor_patterns,
        regexes: BUILTINS
            .iter()
            .map(|builtin| Regex::new(builtin.regex).expect("built-in redaction pattern compiles"))
            .collect(),
    }
});

/// Names that hold credentials wherever they are.
const SENSITIVE_NAMES: &[&str] = &[
    ".env",
    ".env.*",
    "*.env",
    ".envrc",
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_ecdsa_sk",
    "id_ed25519_sk",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "*.jks",
    "*.keystore",
    "*.tfstate",
    "*.tfstate.backup",
    "*.tfvars",
    ".netrc",
    "_netrc",
    ".pgpass",
    ".git-credentials",
    ".npmrc",
    ".pypirc",
    ".htpasswd",
    ".vault-token",
];

/// Of the built-in names, templates and public halves.
const NOT_SENSITIVE_NAMES: &[&str] = &["*.example", "*.sample", "*.template", "*.dist", "*.pub"];

/// Where tools, and the agents themselves, keep logins. Matched under any home directory, so
/// transcripts indexed from another machine or user are judged the same way.
const SENSITIVE_PATHS: &[&str] = &[
    "**/.aws/credentials",
    "**/.aws/sso/cache/**",
    "**/.aws/cli/cache/**",
    "**/gcloud/application_default_credentials.json",
    "**/gcloud/credentials.db",
    "**/gcloud/access_tokens.db",
    "**/gcloud/legacy_credentials/**",
    "**/.azure/msal_token_cache.*",
    "**/.kube/config",
    "**/.docker/config.json",
    "**/gh/hosts.yml",
    "**/glab-cli/config.yml",
    "**/.terraform.d/credentials.tfrc.json",
    "**/.cargo/credentials",
    "**/.cargo/credentials.toml",
    "**/.gem/credentials",
    "**/.gnupg/private-keys-v1.d/**",
    "**/.password-store/**",
    "**/.claude/.credentials.json",
    "**/.codex/auth.json",
    "**/opencode/auth.json",
    "**/.pi/agent/auth.json",
];

fn glob_set(globs: impl IntoIterator<Item = String>) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for glob in globs {
        builder.add(
            GlobBuilder::new(&glob)
                .literal_separator(true)
                .build()
                .with_context(|| format!("invalid sensitive file glob `{glob}`"))?,
        );
    }
    Ok(builder.build()?)
}

/// Files whose contents are credentials, so what reads them out or writes to them is withheld.
struct SensitiveFiles {
    names: GlobSet,
    not_names: GlobSet,
    user_names: GlobSet,
    paths: GlobSet,
}

impl SensitiveFiles {
    /// A user glob without `/` matches a file name anywhere; one with `/` matches a path,
    /// `~/` meaning the home directory and a relative one any suffix.
    fn new(user: &[String]) -> Result<Self> {
        let home = crate::sources::common::home();
        let home = home.to_string_lossy().replace('\\', "/");
        let mut user_names = Vec::new();
        let mut paths: Vec<String> = SENSITIVE_PATHS
            .iter()
            .map(|glob| glob.to_string())
            .collect();
        for glob in user
            .iter()
            .map(|glob| glob.trim())
            .filter(|glob| !glob.is_empty())
        {
            if !glob.contains('/') {
                user_names.push(glob.to_string());
            } else if let Some(rest) = glob.strip_prefix("~/") {
                // Agents write home paths both ways.
                paths.push(format!("{home}/{rest}"));
                paths.push(format!("~/{rest}"));
            } else if glob.starts_with('/') {
                paths.push(glob.to_string());
            } else {
                paths.push(format!("**/{glob}"));
            }
        }
        Ok(Self {
            names: glob_set(SENSITIVE_NAMES.iter().map(|glob| glob.to_string()))?,
            not_names: glob_set(NOT_SENSITIVE_NAMES.iter().map(|glob| glob.to_string()))?,
            user_names: glob_set(user_names)?,
            paths: glob_set(paths)?,
        })
    }

    fn matches(&self, path: &str) -> bool {
        let path = path.trim().replace('\\', "/");
        let path = path.trim_end_matches('/');
        let name = path.rsplit('/').next().unwrap_or(path);
        if name.is_empty() || name == "." || name == ".." {
            return false;
        }
        (self.names.is_match(name) && !self.not_names.is_match(name))
            || self.user_names.is_match(name)
            || self.paths.is_match(path)
    }

    /// Whether `command` names one of these files (`cat .env`, `source ~/.aws/credentials`,
    /// `curl -d @.env`, `--env-file=.env`).
    fn named_in_command(&self, command: &str) -> Option<String> {
        words(command)
            .iter()
            .flat_map(|word| word.split(['|', ';', '&', '(', ')', '<', '>', '`']))
            .map(|word| word.trim_start_matches('@').trim_matches(['"', '\'']))
            .flat_map(|word| {
                let value = word.split_once('=').map(|(_, value)| value);
                std::iter::once(word).chain(value)
            })
            .filter(|word| !word.is_empty() && !word.starts_with('-'))
            .find(|word| self.matches(word))
            .map(str::to_string)
    }
}

/// `$command` run as a command (not merely mentioned): at the start of a line or after a
/// separator, behind `sudo`, `env`, `VAR=value` and the like, by any path.
macro_rules! ran {
    ($command:expr) => {
        concat!(
            r"(?:^|[;&|(`{!\n])[ \t]*(?:(?:sudo|command|exec|time|env|nohup|then|do|else)[ \t]+|\w+=\S*[ \t]+)*(?:\S*/)?",
            $command
        )
    };
}

/// A program's global flags before its subcommand (`aws --profile x configure get`).
macro_rules! then {
    () => {
        r"(?-u:\b)[^\n;&|]*?(?-u:\b)"
    };
}

/// Commands whose output is a credential, a credential store, or the whole environment.
static CREDENTIAL_COMMANDS: Lazy<RegexSet> = Lazy::new(|| {
    let patterns = [
        ran!(r"gh[ \t]+auth[ \t]+token\b"),
        ran!(r"gh[ \t]+auth[ \t]+status\b[^\n;&|]*?[ \t](?:--show-token|-t)\b"),
        ran!(r"glab[ \t]+auth[ \t]+status\b[^\n;&|]*--show-token\b"),
        ran!(
            r"gcloud[ \t]+auth[ \t]+(?:application-default[ \t]+)?print-(?:access|identity)-token\b"
        ),
        ran!(concat!(
            r"aws",
            then!(),
            r"configure[ \t]+(?:get|export-credentials)\b"
        )),
        ran!(concat!(
            r"aws",
            then!(),
            r"sts[ \t]+(?:get-session-token|assume-role\S*|get-federation-token)\b"
        )),
        ran!(concat!(
            r"aws",
            then!(),
            r"(?:ecr|ecr-public)[ \t]+get-login-password\b"
        )),
        ran!(concat!(
            r"aws",
            then!(),
            r"codeartifact[ \t]+get-authorization-token\b"
        )),
        ran!(concat!(
            r"aws",
            then!(),
            r"secretsmanager[ \t]+get-secret-value\b"
        )),
        ran!(concat!(
            r"aws",
            then!(),
            r"ssm[ \t]+get-parameters?\b[^\n;&|]*--with-decryption\b"
        )),
        ran!(r"az[ \t]+account[ \t]+get-access-token\b"),
        ran!(r"az[ \t]+ad[ \t]+sp[ \t]+(?:create-for-rbac|credential[ \t]+reset)\b"),
        ran!(concat!(
            r"kubectl",
            then!(),
            r"(?:get|describe)[ \t]+secrets?\b"
        )),
        ran!(concat!(
            r"kubectl",
            then!(),
            r"config[ \t]+view\b[^\n;&|]*--raw\b"
        )),
        ran!(concat!(r"kubectl", then!(), r"create[ \t]+token\b")),
        ran!(r"vault[ \t]+(?:read|login|print[ \t]+token|token[ \t]+create|kv[ \t]+get)\b"),
        ran!(r"op[ \t]+(?:read|inject|item[ \t]+get|signin\b[^\n;&|]*--raw)\b"),
        ran!(r"pass[ \t]+show\b"),
        ran!(r"gopass[ \t]+(?:show|cat)\b"),
        ran!(r"security[ \t]+find-(?:generic|internet)-password\b[^\n;&|]*[ \t]-[a-zA-Z]*[wg]"),
        ran!(r"secret-tool[ \t]+lookup\b"),
        ran!(r"heroku[ \t]+auth:token\b"),
        ran!(r"doppler[ \t]+secrets\b"),
        ran!(r"npm[ \t]+token[ \t]+create\b"),
        ran!(r"git[ \t]+credential[ \t]+fill\b"),
        ran!(r"terraform[ \t]+output\b[^\n;&|]*[ \t]-(?:json|raw)\b"),
        ran!(r"printenv\b"),
        // The environment and the shell's variables: alone, not running a command.
        ran!(r"(?:env|set|export[ \t]+-p|declare[ \t]+-[a-zA-Z]*[px])[ \t]*(?:\z|[;&|)\n])"),
    ];
    RegexSet::new(patterns.map(|pattern| pattern.replace(r"\b", r"(?-u:\b)")))
        .expect("credential command patterns compile")
});

fn words(command: &str) -> Vec<String> {
    shlex::split(command).unwrap_or_else(|| command.split_whitespace().map(str::to_owned).collect())
}

/// The scripts `command` hands a shell (`bash -lc '<script>'`), which run though quoted.
fn shell_scripts(command: &str) -> Vec<String> {
    let Some(words) = shlex::split(command) else {
        return Vec::new();
    };
    let mut scripts = Vec::new();
    let mut shell = false;
    for (index, word) in words.iter().enumerate() {
        let name = word.rsplit('/').next().unwrap_or(word);
        if matches!(name, "sh" | "bash" | "zsh" | "dash" | "ksh" | "fish") {
            shell = true;
        } else if let Some(script) = shell.then(|| word.strip_prefix("--command=")).flatten() {
            scripts.push(script.to_owned());
            shell = false;
        } else if shell
            && (word == "--command"
                || (word.starts_with('-') && !word.starts_with("--") && word.ends_with('c')))
        {
            scripts.extend(words.get(index + 1).cloned());
            shell = false;
        }
    }
    scripts
}

fn prints_credentials(command: &str) -> bool {
    CREDENTIAL_COMMANDS.is_match(command)
        || shell_scripts(command)
            .iter()
            .any(|script| prints_credentials(script))
}

/// Files a shell command writes by redirecting (`> .env`, `>> log`) or through `tee`.
fn redirected(command: &str) -> Vec<String> {
    let words = words(command);
    let mut files = Vec::new();
    let mut words = words.iter().map(String::as_str);
    let mut teeing = false;
    while let Some(word) = words.next() {
        if matches!(word, "|" | "||" | "&&" | ";" | "&") {
            teeing = false;
            continue;
        }
        if teeing && !word.starts_with('-') {
            files.push(word.to_owned());
            continue;
        }
        if word.rsplit('/').next() == Some("tee") {
            teeing = true;
            continue;
        }
        // `> file`, `2>>file`, `echo x>file`; not a duplicated descriptor (`2>&1`).
        if let Some(at) = word.rfind('>') {
            let target = word[at + 1..].trim_start_matches('|');
            let target = if target.is_empty() {
                words.next().unwrap_or_default()
            } else {
                target
            };
            if !target.is_empty() && !target.starts_with('&') {
                files.push(target.to_owned());
            }
        }
    }
    files
}

const PATCH_HEADERS: &[&str] = &[
    "*** Update File: ",
    "*** Add File: ",
    "*** Delete File: ",
    "*** Move to: ",
];

/// Files a patch names in its headers: `*** Update File:` patches and unified diffs.
fn patched(patch: &str) -> Vec<String> {
    patch
        .lines()
        .filter_map(|line| patch_header_path(line.trim()))
        .map(str::to_owned)
        .collect()
}

fn patch_header_path(line: &str) -> Option<&str> {
    PATCH_HEADERS
        .iter()
        .find_map(|header| line.strip_prefix(header))
        .or_else(|| {
            line.strip_prefix("+++ ")
                .or_else(|| line.strip_prefix("--- "))
                .map(|path| {
                    let path = path.split('\t').next().unwrap_or(path);
                    path.strip_prefix("a/")
                        .or_else(|| path.strip_prefix("b/"))
                        .unwrap_or(path)
                })
                .filter(|path| *path != "/dev/null")
        })
        .map(str::trim)
}

/// Keeps a patch's headers and replaces the hunks of files `sensitive` selects with a marker.
fn withhold_patch(patch: &str, sensitive: &impl Fn(&str) -> bool) -> String {
    let mut out = String::with_capacity(patch.len());
    let mut withholding = false;
    for line in patch.split_inclusive('\n') {
        let trimmed = line.trim();
        if let Some(path) = patch_header_path(trimmed) {
            let was = withholding;
            withholding = sensitive(path);
            out.push_str(line);
            if withholding && !was {
                out.push_str(CONTENT_WITHHELD_MARKER);
                out.push('\n');
            }
            continue;
        }
        if trimmed.starts_with("*** ") || trimmed.starts_with("diff --git ") {
            withholding = false;
        }
        if !withholding {
            out.push_str(line);
        }
    }
    out
}

const SHELL_TOOLS: &[&str] = &[
    "bash",
    "shell",
    "shell_command",
    "exec_command",
    "local_shell",
    "container.exec",
    "run_terminal_cmd",
    "execute_command",
    "run_command",
    "run_shell_command",
    "terminal",
];

const WRITE_TOOLS: &[&str] = &[
    "write",
    "edit",
    "multiedit",
    "notebookedit",
    "write_file",
    "edit_file",
    "create_file",
    "str_replace_editor",
    "str_replace_based_edit_tool",
    "patch",
    "create",
    "apply_diff",
    "replace",
    "search_replace",
];

/// Input keys a single file's path is given under.
const PATH_KEYS: &[&str] = &[
    "file_path",
    "filePath",
    "path",
    "notebook_path",
    "filename",
    "file",
    "target_file",
    "absolute_path",
];
const PATHS_KEYS: &[&str] = &["paths", "file_paths", "filePaths", "files"];
const PATCH_KEYS: &[&str] = &["patchText", "patch_text", "patch", "input", "diff"];
/// Input keys holding what a write tool writes.
const CONTENT_KEYS: &[&str] = &[
    "content",
    "contents",
    "new_string",
    "old_string",
    "new_str",
    "old_str",
    "file_text",
    "text",
    "edits",
    "new_source",
    "code_edit",
    "replacement",
];

/// What a tool call touches.
#[derive(Default)]
struct Access {
    /// Command lines a shell tool runs (the whole line, and any script it hands a shell).
    command: Option<String>,
    reads: Vec<String>,
    writes: Vec<String>,
}

fn tool_key(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    lower.rsplit("__").next().unwrap_or(&lower).to_string()
}

fn command_line(value: &Value) -> Option<String> {
    match value {
        Value::String(line) => Some(line.clone()),
        Value::Array(argv) => {
            let argv = argv.iter().filter_map(Value::as_str).collect::<Vec<_>>();
            // `["bash", "-lc", script]`: the script is what runs.
            Some(shlex::try_join(argv.iter().copied()).unwrap_or_else(|_| argv.join(" ")))
        }
        _ => None,
    }
}

impl Access {
    fn of(tool_name: &str, input: &str) -> Self {
        let tool = tool_key(tool_name);
        let parsed = serde_json::from_str::<Value>(input).ok();
        let object = parsed.as_ref().and_then(Value::as_object);
        let mut access = Self::default();
        if tool == "apply_patch" {
            let patch = object
                .and_then(|object| PATCH_KEYS.iter().find_map(|key| object.get(*key)?.as_str()))
                .or_else(|| parsed.as_ref().and_then(Value::as_str))
                .unwrap_or(input);
            access.writes = patched(patch);
            return access;
        }
        let Some(object) = object else {
            if SHELL_TOOLS.contains(&tool.as_str()) {
                access.writes = redirected(input);
                access.command = Some(input.to_string());
            }
            return access;
        };
        let command = object
            .get("command")
            .or_else(|| object.get("cmd"))
            .and_then(command_line);
        if SHELL_TOOLS.contains(&tool.as_str()) || command.is_some() {
            if let Some(command) = &command {
                // Codex also patches through its shell (`["apply_patch", "*** Begin Patch"]`).
                access.writes = patched(command)
                    .into_iter()
                    .chain(redirected(command))
                    .collect();
            }
            access.command = command;
            return access;
        }
        if let Some(patch) = PATCH_KEYS[..3]
            .iter()
            .find_map(|key| object.get(*key)?.as_str())
        {
            access.writes = patched(patch);
            return access;
        }
        let paths = PATH_KEYS
            .iter()
            .filter_map(|key| object.get(*key)?.as_str())
            .chain(
                PATHS_KEYS
                    .iter()
                    .filter_map(|key| object.get(*key)?.as_array())
                    .flatten()
                    .filter_map(Value::as_str),
            )
            .map(str::to_owned)
            .collect();
        if WRITE_TOOLS.contains(&tool.as_str()) {
            access.writes = paths;
        } else {
            access.reads = paths;
        }
        access
    }
}

/// What redaction does with a tool call beyond pattern redaction.
#[derive(Debug, Default, PartialEq, Eq)]
struct ToolVerdict {
    withhold_output: Option<WithheldOutput>,
    /// The command itself carries a credential, or writes one into a sensitive file.
    drop_input: bool,
    /// The call writes a sensitive file through a write tool or a patch.
    withhold_written: bool,
}

/// Withheld-output verdicts of tool calls whose results have not been seen yet, keyed by
/// source path and call id. Only non-default verdicts are stored, so the common case never
/// takes the lock.
#[derive(Default)]
pub(crate) struct ToolVerdicts {
    entries: Mutex<HashMap<(String, String), WithheldOutput>>,
    len: AtomicUsize,
}

impl ToolVerdicts {
    fn insert(&self, source_path: &str, call_id: &str, verdict: WithheldOutput) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert((source_path.to_string(), call_id.to_string()), verdict);
        self.len.store(entries.len(), Ordering::Release);
    }

    fn take(&self, source_path: &str, call_id: &str) -> Option<WithheldOutput> {
        if self.len.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut entries = self.entries.lock().unwrap();
        let verdict = entries.remove(&(source_path.to_string(), call_id.to_string()));
        self.len.store(entries.len(), Ordering::Release);
        verdict
    }

    /// Restores verdicts persisted on a file's pending calls before it is parsed again.
    pub(crate) fn seed(&self, source_path: &str, pending: &HashMap<String, PendingToolCall>) {
        for (key, call) in pending {
            if let Some(verdict) = call.withheld_output {
                let call_id = call.tool_use_event_id.as_deref().unwrap_or(key);
                self.insert(source_path, call_id, verdict);
            }
        }
    }

    /// Moves verdicts of calls still waiting for their result onto the persisted pending calls.
    pub(crate) fn persist(
        &self,
        source_path: &str,
        pending: &mut HashMap<String, PendingToolCall>,
    ) {
        if self.len.load(Ordering::Acquire) == 0 {
            return;
        }
        for (key, call) in pending.iter_mut() {
            let call_id = call
                .tool_use_event_id
                .clone()
                .unwrap_or_else(|| key.clone());
            if let Some(verdict) = self.take(source_path, &call_id) {
                call.withheld_output = Some(verdict);
            }
        }
    }
}

/// Egress redaction carried in comparable runtime settings; two compare equal when their
/// redaction settings do.
#[derive(Clone, Default)]
pub struct EgressRedaction(Option<std::sync::Arc<Redactor>>);

impl EgressRedaction {
    pub fn new(redactor: Option<std::sync::Arc<Redactor>>) -> Self {
        Self(redactor)
    }

    pub fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        match &self.0 {
            Some(redactor) => redactor.redact(text).text,
            None => Cow::Borrowed(text),
        }
    }
}

impl PartialEq for EgressRedaction {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().map(|redactor| redactor.fingerprint)
            == other.0.as_ref().map(|redactor| redactor.fingerprint)
    }
}

impl Eq for EgressRedaction {}

impl fmt::Debug for EgressRedaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "EgressRedaction(on)"
        } else {
            "EgressRedaction(off)"
        })
    }
}

/// What redacting one record did, for ingest diagnostics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordRedaction {
    pub secrets: u64,
    pub timeouts: u64,
    pub withheld_inputs: u64,
    pub withheld_outputs: u64,
}

impl RecordRedaction {
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// One field's redaction.
pub struct Redacted<'a> {
    pub text: Cow<'a, str>,
    pub secrets: u64,
    pub timed_out: bool,
}

struct Span {
    start: usize,
    end: usize,
    kind: &'static str,
}

fn is_marker(text: &str) -> bool {
    MARKER_PREFIXES
        .iter()
        .any(|prefix| text.starts_with(prefix))
}

/// A named value that reads as a credential rather than a word, a number or a stand-in.
fn looks_like_secret(value: &str) -> bool {
    if value.len() < 8 {
        return false;
    }
    let lower = value.to_ascii_lowercase();
    if value.starts_with(['$', '<', '{', '%', '*', '[', '('])
        || value.contains("...")
        || [
            "example",
            "placeholder",
            "changeme",
            "your_",
            "your-",
            "xxxx",
            "redacted",
            "process.env",
            "os.environ",
        ]
        .iter()
        .any(|word| lower.contains(word))
    {
        return false;
    }
    let classes = [
        value.bytes().any(|byte| byte.is_ascii_lowercase()),
        value.bytes().any(|byte| byte.is_ascii_uppercase()),
        value.bytes().any(|byte| byte.is_ascii_digit()),
        value.bytes().any(|byte| !byte.is_ascii_alphanumeric()),
    ];
    classes.iter().filter(|present| **present).count() >= 2
}

/// Redacts credentials in transcript text and judges tool calls. Built once per ingest or
/// server from `[redaction]` config.
pub struct Redactor {
    enabled: bool,
    user: Vec<Regex>,
    user_set: Option<RegexSet>,
    sensitive_files: SensitiveFiles,
    fingerprint: [u8; 32],
    budget: Duration,
}

impl fmt::Debug for Redactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Redactor")
            .field("enabled", &self.enabled)
            .field("user_patterns", &self.user.len())
            .finish_non_exhaustive()
    }
}

impl Redactor {
    /// Index-time redaction off: records are indexed as parsed and parser versions are
    /// unchanged.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::new(&[], &[]).expect("built-in redaction compiles")
        }
    }

    pub fn new(patterns: &[String], sensitive_files: &[String]) -> Result<Self> {
        let user = patterns
            .iter()
            .map(|pattern| {
                Regex::new(pattern)
                    .with_context(|| format!("invalid [redaction] pattern `{pattern}`"))
            })
            .collect::<Result<Vec<_>>>()?;
        let user_set = (!user.is_empty())
            .then(|| RegexSet::new(patterns))
            .transpose()?;
        let mut hash = Sha256::new();
        hash.update(b"memex-redaction");
        hash.update(REDACTION_VERSION.to_le_bytes());
        for (label, values) in [("patterns", patterns), ("sensitive_files", sensitive_files)] {
            hash.update(label.as_bytes());
            for value in values {
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value.as_bytes());
            }
        }
        Ok(Self {
            enabled: true,
            user,
            user_set,
            sensitive_files: SensitiveFiles::new(sensitive_files)?,
            fingerprint: hash.finalize().into(),
            budget: FIELD_BUDGET,
        })
    }

    #[cfg(test)]
    fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = budget;
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The parser version a file's checkpoint records under these settings. Disabled leaves
    /// `base` as is; enabled maps every (base, settings) pair to a distinct value with the high
    /// bit set, which base versions never use, so any change reparses.
    pub fn fold_version(&self, base: u32) -> u32 {
        if !self.enabled {
            return base;
        }
        let mut hash = Sha256::new();
        hash.update(self.fingerprint);
        hash.update(base.to_le_bytes());
        let digest = hash.finalize();
        0x8000_0000
            | (u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]) & 0x7fff_ffff)
    }

    fn spans(&self, text: &str, deadline: Instant) -> Option<Vec<Span>> {
        let compiled = &*COMPILED;
        let mut candidates = vec![false; BUILTINS.len()];
        for found in compiled.anchors.find_overlapping_iter(text) {
            for pattern in &compiled.anchor_patterns[found.pattern().as_usize()] {
                candidates[*pattern] = true;
            }
        }
        let mut spans = Vec::new();
        for (index, builtin) in BUILTINS.iter().enumerate() {
            if !candidates[index] {
                continue;
            }
            for captures in compiled.regexes[index].captures_iter(text) {
                let Some(secret) = captures.name("secret") else {
                    continue;
                };
                let value = secret.as_str();
                if value.is_empty()
                    || is_marker(value)
                    || value.bytes().all(|byte| byte == b'*')
                    || (builtin.validate && !looks_like_secret(value))
                {
                    continue;
                }
                spans.push(Span {
                    start: secret.start(),
                    end: secret.end(),
                    kind: builtin.kind,
                });
            }
            if Instant::now() > deadline {
                return None;
            }
        }
        if let Some(set) = &self.user_set {
            for index in set.matches(text).iter() {
                for captures in self.user[index].captures_iter(text) {
                    let Some(secret) = captures.name("secret").or_else(|| captures.get(0)) else {
                        continue;
                    };
                    if secret.is_empty() || is_marker(secret.as_str()) {
                        continue;
                    }
                    spans.push(Span {
                        start: secret.start(),
                        end: secret.end(),
                        kind: "custom",
                    });
                }
                if Instant::now() > deadline {
                    return None;
                }
            }
        }
        Some(spans)
    }

    /// Replaces every credential in `text` with `[redacted:<kind>]`, regardless of whether
    /// index-time redaction is enabled (egress uses this directly).
    pub fn redact<'a>(&self, text: &'a str) -> Redacted<'a> {
        let deadline = Instant::now() + self.budget;
        let mut current = Cow::Borrowed(text);
        let mut secrets = 0;
        for _ in 0..MAX_PASSES {
            let Some(mut spans) = self.spans(&current, deadline) else {
                return Redacted {
                    text: Cow::Owned(TIMED_OUT_MARKER.to_string()),
                    secrets,
                    timed_out: true,
                };
            };
            if spans.is_empty() {
                break;
            }
            spans.sort_by_key(|span| (span.start, std::cmp::Reverse(span.end)));
            let mut out = String::with_capacity(current.len());
            let mut cursor = 0;
            let mut index = 0;
            while index < spans.len() {
                let start = spans[index].start.max(cursor);
                let kind = spans[index].kind;
                let mut end = spans[index].end;
                index += 1;
                while index < spans.len() && spans[index].start < end {
                    end = end.max(spans[index].end);
                    index += 1;
                }
                if end <= start {
                    continue;
                }
                out.push_str(&current[cursor..start]);
                out.push_str("[redacted:");
                out.push_str(kind);
                out.push(']');
                cursor = end;
                secrets += 1;
            }
            out.push_str(&current[cursor..]);
            current = Cow::Owned(out);
        }
        Redacted {
            text: current,
            secrets,
            timed_out: false,
        }
    }

    fn redact_field(&self, field: &mut String, stats: &mut RecordRedaction) {
        let redacted = self.redact(field);
        stats.secrets += redacted.secrets;
        stats.timeouts += u64::from(redacted.timed_out);
        if let Cow::Owned(text) = redacted.text {
            *field = text;
        }
    }

    /// Redacts every string in an egress payload. Returns the number of secrets replaced.
    pub fn redact_json(&self, value: &mut Value) -> u64 {
        match value {
            Value::String(text) => {
                let redacted = self.redact(text);
                let secrets = redacted.secrets + u64::from(redacted.timed_out);
                if let Cow::Owned(replacement) = redacted.text {
                    *text = replacement;
                }
                secrets
            }
            Value::Array(items) => items.iter_mut().map(|item| self.redact_json(item)).sum(),
            Value::Object(map) => map.values_mut().map(|item| self.redact_json(item)).sum(),
            _ => 0,
        }
    }

    /// Whether `text` holds anything redaction would replace; a scan over budget counts.
    fn contains_secret(&self, text: &str) -> bool {
        self.spans(text, Instant::now() + self.budget)
            .is_none_or(|spans| !spans.is_empty())
    }

    fn judge(&self, tool_name: Option<&str>, input: &str) -> ToolVerdict {
        let access = Access::of(tool_name.unwrap_or_default(), input);
        let sensitive = |path: &str| self.sensitive_files.matches(path);
        let mut verdict = ToolVerdict::default();
        if let Some(command) = &access.command {
            if prints_credentials(command) {
                verdict.withhold_output = Some(WithheldOutput::CredentialCommand);
            } else if self.sensitive_files.named_in_command(command).is_some() {
                verdict.withhold_output = Some(WithheldOutput::SensitiveFile);
            }
            // A command written with a credential in it, or one writing into a sensitive file,
            // carries the secret in its own text.
            if self.contains_secret(command)
                || redirected(command).iter().any(|path| sensitive(path))
            {
                verdict.drop_input = true;
            }
            if access.writes.iter().any(|path| sensitive(path)) {
                verdict.drop_input = true;
                verdict.withhold_output = Some(WithheldOutput::SensitiveFile);
            }
            return verdict;
        }
        if access.reads.iter().any(|path| sensitive(path)) {
            verdict.withhold_output = Some(WithheldOutput::SensitiveFile);
        }
        if access.writes.iter().any(|path| sensitive(path)) {
            verdict.withhold_written = true;
            // A write tool's result can echo the file (an edit's snippet).
            verdict.withhold_output = Some(WithheldOutput::SensitiveFile);
        }
        verdict
    }

    /// The input of a write to a sensitive file with its written content replaced.
    fn withhold_written(&self, input: &str) -> String {
        let sensitive = |path: &str| self.sensitive_files.matches(path);
        let Ok(mut value) = serde_json::from_str::<Value>(input) else {
            return withhold_patch(input, &sensitive);
        };
        match &mut value {
            Value::String(patch) => *patch = withhold_patch(patch, &sensitive),
            Value::Object(object) => {
                for (key, item) in object.iter_mut() {
                    if PATCH_KEYS.contains(&key.as_str())
                        && let Value::String(patch) = item
                    {
                        *patch = withhold_patch(patch, &sensitive);
                    } else if CONTENT_KEYS.contains(&key.as_str()) {
                        *item = Value::String(CONTENT_WITHHELD_MARKER.to_string());
                    }
                }
            }
            _ => {}
        }
        serde_json::to_string(&value).unwrap_or_else(|_| CONTENT_WITHHELD_MARKER.to_string())
    }

    /// Redacts one record in place before it is clipped and indexed. `verdicts` carries
    /// withheld-output verdicts from a tool call to its later result.
    pub(crate) fn redact_record(
        &self,
        record: &mut Record,
        verdicts: &ToolVerdicts,
    ) -> RecordRedaction {
        let mut stats = RecordRedaction::default();
        if !self.enabled {
            return stats;
        }
        let text_is_input = record.tool_input.as_deref() == Some(record.text.as_str());
        let text_is_output = record.tool_output.as_deref() == Some(record.text.as_str());
        let verdict = record
            .tool_input
            .as_deref()
            .map(|input| self.judge(record.tool_name.as_deref(), input))
            .unwrap_or_default();
        // Parsers link a result to its call by the call's id; some (Kiro) keep the native call id
        // there and the call record's event id in `parent_event_id`.
        let paired = if record.role == "tool_result" {
            [
                record.links.parent_tool_use_id.as_deref(),
                record.links.parent_event_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            .find_map(|call_id| verdicts.take(&record.source_path, call_id))
        } else {
            None
        };
        let written = verdict
            .withhold_written
            .then(|| {
                record
                    .tool_input
                    .as_deref()
                    .map(|input| self.withhold_written(input))
            })
            .flatten();

        self.redact_field(&mut record.text, &mut stats);
        for field in [
            record.tool_input.as_mut(),
            record.tool_output.as_mut(),
            record.links.source_content.as_mut(),
        ]
        .into_iter()
        .flatten()
        {
            self.redact_field(field, &mut stats);
        }

        if verdict.drop_input {
            record.tool_input = Some(INPUT_WITHHELD_MARKER.to_string());
            if text_is_input || record.role == "tool_use" {
                record.text = INPUT_WITHHELD_MARKER.to_string();
            }
            stats.withheld_inputs += 1;
        } else if let Some(written) = written {
            // Pattern redaction runs on what is kept, the paths and any other fields.
            let mut written = written;
            self.redact_field(&mut written, &mut stats);
            if text_is_input {
                record.text = written.clone();
            } else if record.role == "tool_use" {
                record.text = CONTENT_WITHHELD_MARKER.to_string();
            }
            record.tool_input = Some(written);
            stats.withheld_inputs += 1;
        }
        let withheld = if record.tool_output.is_some() && record.role != "tool_result" {
            // One record holds both the call and its result (OpenCode).
            verdict.withhold_output
        } else {
            if let (Some(reason), Some(call_id)) =
                (verdict.withhold_output, record.links.event_id.as_deref())
            {
                verdicts.insert(&record.source_path, call_id, reason);
            }
            paired
        };
        if let Some(reason) = withheld {
            if record.tool_output.is_some() {
                record.tool_output = Some(reason.marker().to_string());
            }
            if record.role == "tool_result" || text_is_output {
                record.text = reason.marker().to_string();
            }
            stats.withheld_outputs += 1;
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    // Fake credentials are assembled at runtime so no contiguous token sits in the source
    // for secret scanners to flag.
    fn fake(prefix: &str, body: &str, repeat: usize) -> String {
        format!("{prefix}{}", body.repeat(repeat))
    }

    fn redactor() -> Redactor {
        Redactor::new(&[], &[]).unwrap()
    }

    fn redacted(text: &str) -> String {
        redactor().redact(text).text.into_owned()
    }

    #[test]
    fn built_in_formats_replace_only_the_secret() {
        let cases = [
            (fake("gh", "p_", 1) + &"aB3".repeat(12), "github-token"),
            (
                fake("github_", "pat_", 1) + &"Ab9_".repeat(18),
                "github-token",
            ),
            (
                fake("sk-ant-", "api03-", 1) + &"x9Y_".repeat(20),
                "anthropic-api-key",
            ),
            (
                fake("sk-", "proj-", 1) + &"Ab3d".repeat(12),
                "openai-api-key",
            ),
            (
                fake("AK", "IA", 1) + "ABCDEFGHIJ234567",
                "aws-access-key-id",
            ),
            (
                fake("xo", "xb-", 1) + "1234567890-abcdefABCDEF",
                "slack-token",
            ),
            (fake("sk_", "live_", 1) + &"a1B2".repeat(7), "stripe-key"),
            (fake("AI", "za", 1) + &"Sy0_a".repeat(7), "google-api-key"),
            (fake("glp", "at-", 1) + &"a1b2C".repeat(4), "gitlab-token"),
            (fake("np", "m_", 1) + &"a1B2".repeat(9), "npm-token"),
        ];
        for (secret, kind) in cases {
            let text = format!("token is {secret} here");
            let out = redacted(&text);
            assert_eq!(out, format!("token is [redacted:{kind}] here"), "{kind}");
        }
    }

    #[test]
    fn structural_and_named_values_keep_their_names() {
        let jwt = format!(
            "{}.{}.{}",
            fake("eyJ", "hbGciOiJI", 2),
            fake("eyJ", "zdWIiOiIx", 2),
            "c2lnbmF0dXJlX19f"
        );
        let pem_body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC".repeat(3);
        let pem = format!(
            "-----BEGIN {k}-----\n{pem_body}\n-----END {k}-----",
            k = "PRIVATE KEY"
        );
        let cases = [
            (
                format!("Authorization: Bearer {jwt}"),
                "Authorization: Bearer [redacted:jwt]".to_string(),
            ),
            (
                format!("key:\n{pem}\ndone"),
                "key:\n-----BEGIN PRIVATE KEY-----[redacted:private-key]-----END PRIVATE KEY-----\ndone"
                    .to_string(),
            ),
            (
                "postgres://app:Sup3r-s3cret@db.internal:5432/app".to_string(),
                "postgres://app:[redacted:url-password]@db.internal:5432/app".to_string(),
            ),
            (
                "Server=db;User Id=sa;Password=Hunter2x;".to_string(),
                "Server=db;User Id=sa;Password=[redacted:connection-string-password];".to_string(),
            ),
            (
                "export DEPLOY_TOKEN=q8Zr4kLm2Np9".to_string(),
                "export DEPLOY_TOKEN=[redacted:credential-assignment]".to_string(),
            ),
            (
                r#"{"OPENAI_API_KEY": "not-a-real-key-42"}"#.to_string(),
                r#"{"OPENAI_API_KEY": "[redacted:credential-assignment]"}"#.to_string(),
            ),
            (
                r#"{"refresh_token":"rt-9f8e7d6c5b4a"}"#.to_string(),
                r#"{"refresh_token":"[redacted:credential-field]"}"#.to_string(),
            ),
            (
                r#"{\"access_token\":\"at-1a2b3c4d5e\"}"#.to_string(),
                r#"{\"access_token\":\"[redacted:credential-field]\"}"#.to_string(),
            ),
            (
                "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                "aws_secret_access_key = [redacted:aws-secret-access-key]".to_string(),
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(redacted(&input), expected, "{input}");
        }
    }

    #[test]
    fn prose_code_and_placeholders_are_kept() {
        for text in [
            "Bearer token required for this endpoint",
            "MAX_TOKENS=4096",
            "API_KEY: Optional[str] = None",
            "DB_PASSWORD=${DB_PASSWORD}",
            "GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}",
            r#"if line == "-----BEGIN PRIVATE KEY-----" { start() }"#,
            "visit https://example.com:8080/path@section",
            "ssh://git@github.com:22/org/repo.git",
            "the task-1234 and sk-learn docs",
            r#"{"token": "short"}"#,
            r#"{"password": "<your password here>"}"#,
            "postgres://user:****@localhost/db",
            "API_TOKEN=your-api-token-here",
            "let token = request.headers.get(\"authorization\");",
        ] {
            let redaction = redactor().redact(text);
            assert!(
                matches!(redaction.text, Cow::Borrowed(_)),
                "should keep {text}, got {}",
                redaction.text
            );
        }
    }

    #[test]
    fn redaction_is_idempotent_and_merges_overlaps() {
        let token = fake("gh", "p_", 1) + &"Zz9".repeat(12);
        let text = format!("GITHUB_TOKEN={token} and again {token}");
        let once = redacted(&text);
        assert_eq!(
            once,
            "GITHUB_TOKEN=[redacted:github-token] and again [redacted:github-token]"
        );
        assert!(matches!(redactor().redact(&once).text, Cow::Borrowed(_)));
    }

    #[test]
    fn user_patterns_replace_their_secret_group_or_whole_match() {
        let redactor = Redactor::new(
            &[
                r"corp_[a-z0-9]{12}".to_string(),
                r"acct=(?<secret>\d{6})".to_string(),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(
            redactor
                .redact("use corp_abc123def456 for acct=123456")
                .text,
            "use [redacted:custom] for acct=[redacted:custom]"
        );
        assert!(Redactor::new(&["(".to_string()], &[]).is_err());
    }

    #[test]
    fn over_budget_fields_fail_closed() {
        let redactor = redactor().with_budget(Duration::ZERO);
        let text = format!("x {} y", fake("gh", "p_", 1) + &"Zz9".repeat(12));
        let redaction = redactor.redact(&text);
        assert!(redaction.timed_out);
        assert_eq!(redaction.text, TIMED_OUT_MARKER);
        // A field with nothing to look at costs nothing and is kept.
        assert!(!redactor.redact("plain words").timed_out);
    }

    #[test]
    fn versions_change_only_with_enabled_settings() {
        let base = crate::sources::index_state_version(SourceKind::Claude);
        assert_eq!(Redactor::disabled().fold_version(base), base);
        let enabled = redactor().fold_version(base);
        assert_ne!(enabled, base);
        assert!(enabled & 0x8000_0000 != 0);
        assert_eq!(enabled, redactor().fold_version(base));
        assert_ne!(enabled, redactor().fold_version(base + 1));
        let custom = Redactor::new(&["corp_[a-z]+".to_string()], &[]).unwrap();
        assert_ne!(enabled, custom.fold_version(base));
        let files = Redactor::new(&[], &["*.secret".to_string()]).unwrap();
        assert_ne!(enabled, files.fold_version(base));
    }

    fn judge(tool: &str, input: serde_json::Value) -> ToolVerdict {
        redactor().judge(Some(tool), &input.to_string())
    }

    #[test]
    fn credential_commands_and_sensitive_reads_withhold_output() {
        for command in [
            "gh auth token",
            "cd repo && kubectl --context prod get secret db -o yaml",
            "aws --profile ops configure export-credentials",
            "op read op://vault/item/password",
            "sudo printenv",
            "env",
            "cat .env",
            "grep KEY ~/.aws/credentials",
            "source ./config/.env.local",
            "cat ~/.codex/auth.json",
            "bash -lc 'gh auth token'",
        ] {
            assert!(
                judge("Bash", serde_json::json!({ "command": command }))
                    .withhold_output
                    .is_some(),
                "{command}"
            );
        }
        let codex = judge(
            "shell",
            serde_json::json!({ "command": ["bash", "-lc", "vault kv get secret/app"] }),
        );
        assert_eq!(
            codex.withhold_output,
            Some(WithheldOutput::CredentialCommand)
        );
        for command in [
            "echo gh auth token is how you print it",
            "cat .env.example",
            "cat ~/.ssh/id_ed25519.pub",
            "ls -la",
            "cargo test",
            "python -m venv .venv",
        ] {
            assert_eq!(
                judge("Bash", serde_json::json!({ "command": command })),
                ToolVerdict::default(),
                "{command}"
            );
        }
        for path in [
            "/home/dev/.claude/.credentials.json",
            "~/.local/share/opencode/auth.json",
            "/Users/dev/.pi/agent/auth.json",
            "deploy/prod.tfvars",
            "/home/dev/.ssh/id_rsa",
        ] {
            assert_eq!(
                judge("Read", serde_json::json!({ "file_path": path })).withhold_output,
                Some(WithheldOutput::SensitiveFile),
                "{path}"
            );
        }
        assert_eq!(
            judge("Read", serde_json::json!({ "file_path": "src/main.rs" })),
            ToolVerdict::default()
        );
    }

    #[test]
    fn user_sensitive_files_extend_the_built_ins() {
        let redactor = Redactor::new(
            &[],
            &["*.secret".to_string(), "config/credentials.yml".to_string()],
        )
        .unwrap();
        for path in ["app/db.secret", "/srv/app/config/credentials.yml"] {
            let input = serde_json::json!({ "file_path": path }).to_string();
            assert_eq!(
                redactor.judge(Some("Read"), &input).withhold_output,
                Some(WithheldOutput::SensitiveFile),
                "{path}"
            );
        }
    }

    #[test]
    fn writes_to_sensitive_files_keep_paths_and_withhold_content() {
        let redactor = redactor();
        let input = serde_json::json!({
            "file_path": "/repo/.env",
            "content": "PLAIN_VALUE=abc123xyz",
        })
        .to_string();
        let verdict = redactor.judge(Some("Write"), &input);
        assert!(verdict.withhold_written);
        let written = redactor.withhold_written(&input);
        assert!(written.contains("/repo/.env"));
        assert!(!written.contains("abc123xyz"));
        assert!(written.contains(CONTENT_WITHHELD_MARKER));

        let patch = "*** Begin Patch\n*** Update File: src/lib.rs\n+fn ok() {}\n*** Add File: .env\n+PLAIN_VALUE=abc123xyz\n*** End Patch\n";
        assert!(redactor.judge(Some("apply_patch"), patch).withhold_written);
        let written = redactor.withhold_written(patch);
        assert!(written.contains("+fn ok() {}"));
        assert!(written.contains("*** Add File: .env"));
        assert!(!written.contains("abc123xyz"));

        // A shell redirect carries the content in the command itself.
        let shell = judge(
            "Bash",
            serde_json::json!({ "command": "echo PLAIN=v1 > .env" }),
        );
        assert!(shell.drop_input);
    }

    #[test]
    fn commands_carrying_a_credential_drop_their_input() {
        let token = fake("gh", "p_", 1) + &"Qq7".repeat(12);
        let verdict = judge(
            "Bash",
            serde_json::json!({ "command": format!("curl -H 'x: {token}' https://api.example") }),
        );
        assert!(verdict.drop_input);
    }

    fn record(role: &str, text: &str, input: Option<&str>, output: Option<&str>) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id: 1,
            ts: 0,
            project: "p".into(),
            session_id: "s".into(),
            turn_id: 0,
            role: role.into(),
            text: text.into(),
            tool_name: Some("Bash".into()),
            tool_input: input.map(str::to_string),
            tool_output: output.map(str::to_string),
            links: RecordLinks::default(),
            source_path: "/t/session.jsonl".into(),
        }
    }

    fn tool_use(command: &str, call_id: &str) -> Record {
        let input = serde_json::json!({ "command": command }).to_string();
        let mut record = record("tool_use", &input, Some(&input), None);
        record.links.event_id = Some(call_id.into());
        record
    }

    fn tool_result(output: &str, call_id: &str) -> Record {
        let mut record = record("tool_result", output, None, Some(output));
        record.links.parent_tool_use_id = Some(call_id.into());
        record
    }

    #[test]
    fn paired_results_are_withheld_and_other_output_stays_searchable() {
        let redactor = redactor();
        let verdicts = ToolVerdicts::default();
        let mut call = tool_use("cat .env", "call-1");
        let mut other = tool_use("cargo test", "call-2");
        assert_eq!(
            redactor
                .redact_record(&mut call, &verdicts)
                .withheld_outputs,
            0
        );
        redactor.redact_record(&mut other, &verdicts);
        assert!(
            call.text.contains("cat .env"),
            "the call itself stays readable"
        );

        let mut result = tool_result("PLAIN_VALUE=abc123xyz", "call-1");
        let stats = redactor.redact_record(&mut result, &verdicts);
        assert_eq!(stats.withheld_outputs, 1);
        assert_eq!(result.text, "[output withheld: sensitive file]");
        assert_eq!(result.tool_output.as_deref(), Some(result.text.as_str()));
        assert_eq!(result.tool_name.as_deref(), Some("Bash"));

        let mut passed = tool_result("test result: ok. 12 passed", "call-2");
        redactor.redact_record(&mut passed, &verdicts);
        assert_eq!(passed.text, "test result: ok. 12 passed");
    }

    #[test]
    fn verdicts_survive_an_incremental_pass_through_pending_calls() {
        let redactor = redactor();
        let first_pass = ToolVerdicts::default();
        let mut call = tool_use("gh auth token", "call-9");
        redactor.redact_record(&mut call, &first_pass);
        let mut pending = HashMap::from([(
            "call-9".to_string(),
            PendingToolCall {
                tool_use_event_id: Some("call-9".into()),
                ..PendingToolCall::default()
            },
        )]);
        first_pass.persist("/t/session.jsonl", &mut pending);
        assert_eq!(
            pending["call-9"].withheld_output,
            Some(WithheldOutput::CredentialCommand)
        );

        // The checkpoint round-trips the verdict.
        let pending: HashMap<String, PendingToolCall> =
            serde_json::from_str(&serde_json::to_string(&pending).unwrap()).unwrap();
        let second_pass = ToolVerdicts::default();
        second_pass.seed("/t/session.jsonl", &pending);
        let mut result = tool_result("gho_example_output_value", "call-9");
        redactor.redact_record(&mut result, &second_pass);
        assert_eq!(result.text, "[output withheld: credential command]");
    }

    #[test]
    fn combined_call_and_result_records_are_withheld_in_place() {
        let redactor = redactor();
        let input = serde_json::json!({ "filePath": "/repo/.env" }).to_string();
        let mut combined = record("assistant", "", Some(&input), Some("PLAIN_VALUE=abc123xyz"));
        combined.tool_name = Some("read".into());
        redactor.redact_record(&mut combined, &ToolVerdicts::default());
        assert_eq!(
            combined.tool_output.as_deref(),
            Some("[output withheld: sensitive file]")
        );
        assert_eq!(combined.tool_input.as_deref(), Some(input.as_str()));
    }

    #[test]
    fn disabled_redaction_leaves_records_untouched() {
        let token = fake("gh", "p_", 1) + &"Zz9".repeat(12);
        let mut call = tool_use(&format!("echo {token}"), "call-1");
        let before = call.text.clone();
        let stats = Redactor::disabled().redact_record(&mut call, &ToolVerdicts::default());
        assert!(stats.is_empty());
        assert_eq!(call.text, before);
    }

    #[test]
    fn egress_json_is_redacted_in_every_string() {
        let token = fake("gh", "p_", 1) + &"Zz9".repeat(12);
        let mut value = serde_json::json!({
            "records": [{ "text": format!("pushed with {token}"), "doc_id": 7 }],
            "machine": "local",
        });
        assert_eq!(redactor().redact_json(&mut value), 1);
        assert_eq!(
            value["records"][0]["text"],
            "pushed with [redacted:github-token]"
        );
        assert_eq!(value["records"][0]["doc_id"], 7);
    }
}
