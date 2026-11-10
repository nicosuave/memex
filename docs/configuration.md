# Configuration and embeddings

[Back to Memex](../README.md)

## Embeddings

Enable during indexing:
```
memex index --embeddings
```

Recommended when embeddings are on (especially non-`potion` models): run the background
daemon with `memex daemon enable --continuous`, and consider setting `auto_index_on_search = false`
to keep searches fast.

The `embeddings` config key selects the backend:

| Value | Meaning |
|-------|---------|
| `false` | Embeddings off |
| `true` or `"local"` | Local fastembed or model2vec model |
| `"remote"` | OpenAI-compatible HTTP API (see [Remote embeddings](#remote-embeddings)) |

Changing the model (or, for remote, `embedding_dimensions`) re-embeds on the next `memex index`,
`memex embed`, or daemon embed cycle. Vectors from different models or sizes are never mixed.
## Embedding model

Select via `--model` flag or `MEMEX_MODEL` env var. For local embeddings, the value is one
of these aliases:

| Model | Dims | Speed | Quality |
|-------|------|-------|---------|
| minilm | 384 | Fastest | Good |
| bge | 384 | Fast | Better |
| nomic | 768 | Moderate | Good |
| gemma | 768 | Slowest | Best |
| potion | 256 | Fastest (tiny) | Lowest |

```
memex index --model minilm
# or
MEMEX_MODEL=minilm memex index
```

or any fastembed model name, matched case-insensitively against the `EmbeddingModel`
variant names, such as `BGELargeENV15` or `multilinguale5base`. See
[fastembed's `EmbeddingModel` list](https://docs.rs/fastembed/5.17.4/fastembed/enum.EmbeddingModel.html)
for the catalog; an unknown name fails with the list of supported models. Each local model
produces its native vector size.

A fastembed model downloads once into `<root>/embed-cache` (`~/.memex/embed-cache`, or under
`--root`), shared by the CLI, MCP server, TUI, and daemon. `HF_HOME` overrides that
directory, and `FASTEMBED_CACHE_DIR` does when `HF_HOME` is unset. Memex no longer
downloads into a `.fastembed_cache` directory in the current working directory. Existing
ones are left alone; you can delete them, or move their `models--*` directories into
`embed-cache` to skip the download. `potion` downloads into the Hugging Face cache,
`~/.cache/huggingface/hub`.
## Remote embeddings

With `embeddings = "remote"`, memex computes embeddings with any OpenAI-compatible
server (OpenAI, Ollama, vLLM, LM Studio, LiteLLM).
`model`, `MEMEX_MODEL`, or `--model` names the remote model, passed verbatim.

| Key | Default | Meaning |
|-----|---------|---------|
| `model` | none | Remote model name; required |
| `embedding_base_url` | none | `http` or `https` API base URL; required |
| `embedding_api_key` | none | Literal API key |
| `embedding_api_key_env` | none | Variable holding the API key |
| `embedding_dimensions` | model default | API `dimensions` parameter (up to 65536); remote only |
| `embedding_batch_size` | 64 | Texts per request (1 to 2048); local too |
| `embedding_timeout_secs` | 60 | Per-attempt timeout in seconds (1 to 600) |
| `embedding_max_retries` | 3 | Retries on 429, 5xx, timeouts, and connection errors (0 to 10) |

The key is `embedding_api_key`, else only the variable named by `embedding_api_key_env`, else
`MEMEX_EMBEDDING_API_KEY`, then `OPENAI_API_KEY` (sent to any host); with no key, no
`Authorization` header is sent. `execution_provider`, `compute_units`, and `cuda_*` are ignored
in remote mode; the URL, key, timeout, and retry keys are ignored in local mode.

```toml
embeddings = "remote"
model = "text-embedding-3-small"
embedding_base_url = "https://api.openai.com/v1"
```

```toml
embeddings = "remote"
model = "nomic-embed-text"
embedding_base_url = "http://localhost:11434/v1"  # Ollama
```

A startup probe sets or checks the vector size, and every response is checked against it.
A server that starts returning a different size is detected by `memex embed`, not by the
daemon. Switching provider with the same model name and size is not detected; run
`memex embed` after such a switch.

memex sends transcript text (user and assistant messages, each truncated to 8192 bytes) and
memory documents to the endpoint, with credentials redacted first (see
[Secret redaction](#secret-redaction)). Plain `http://` to a non-loopback host is rejected when a
key is set and warned about otherwise; `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` are
honored.

## Secret redaction

memex removes credentials from what it sends off the machine and, if you opt in, from what it
indexes. Source transcripts are never modified; redaction is a property of the derived index.

```toml
[redaction]
enabled = true          # opt in to index-time redaction (default false)
egress = true           # MCP over HTTP and remote embedding requests (default true)
patterns = ["corp_[A-Za-z0-9]{32}", "acct=(?<secret>\\d{10})"]
sensitive_files = ["*.secret", "~/work/creds/**"]
```

With `enabled = true`, every record's text, tool input and tool output is scanned at index
time, before tool payloads are clipped, for known credential formats: provider API keys and
tokens (Anthropic, OpenAI, GitHub, GitLab, Slack, Stripe, Google, AWS, Hugging Face, npm, PyPI,
Vault, and others), private key blocks, JWTs, `Bearer` and `Authorization` headers, passwords
in URLs and connection strings, and literal values assigned to names such as `DEPLOY_TOKEN` or
`"client_secret"`. Only the value is replaced, with a marker such as `[redacted:github-token]`,
so the surrounding text stays searchable (search `redacted` to find them). `patterns` adds
regexes; a pattern with a `secret` capture group replaces only that group, otherwise the whole
match. An invalid pattern is a config error.

Tool calls are also judged by what they do. The output of commands that print credentials (`gh
auth token`, `kubectl get secret`, `aws configure export-credentials`, `op read`, `vault kv get`,
`printenv`, and similar, including inside `bash -c`) and of reads of sensitive files is replaced
with `[output withheld: ...]`; the call itself, its tool name and its result record stay indexed.
Sensitive files are `.env*`, `*.env`, `.envrc`, SSH private keys, `*.pem`, `*.key`, `*.tfvars`,
`*.tfstate`, `.netrc`, `.npmrc`, `.pypirc`, `~/.aws/credentials`, `~/.kube/config`, and the
agents' own logins (`~/.claude/.credentials.json`, `~/.codex/auth.json`, OpenCode and Pi
`auth.json`), but not templates (`*.example`, `*.sample`, `*.template`, `*.dist`) or public keys
(`*.pub`). `sensitive_files` adds globs: one without `/` matches a file name anywhere, one with
`/` matches a path (`~/` is the home directory). A write or patch to a sensitive file keeps the
path and replaces the written content; a shell command that itself contains a credential, or
writes into a sensitive file, has its input replaced. All other tool output stays fully
indexed and searchable.

A field whose redaction takes longer than a second is replaced with `[redaction timed out]`.
`memex index` reports `redacted_secrets`, `withheld_tool_inputs`, `withheld_tool_outputs`, and
`redaction_timeouts` with its other diagnostics.

Index-time redaction is off by default. Off, records are indexed as parsed and every file's
index checkpoint is the same as in memex releases without redaction, so upgrading reparses
nothing. Turning it on reparses all indexed history on the next index run and, with embeddings
enabled, re-embeds it; expect it to take as long as a first index. While it is on, so does
changing `patterns` or `sensitive_files`, or upgrading to a memex release whose built-in rules
changed. Turning it off again re-derives the unredacted records the same way.

`egress` is on by default and never reparses. It applies the same patterns (not the tool rules)
to MCP responses served over HTTP, including results proxied from other machines and records
indexed while index-time redaction is off, and to every remote embedding request. It also
covers what [`memex dream`](dreaming.md) sends to its executor and the excerpts it stores. It is
independent of `enabled`, so the local index can stay exact while nothing unredacted leaves the
machine; `patterns` apply to egress whether or not `enabled` is set. The stdio MCP server, the
CLI, the TUI, and the loopback web UI read the index as it is.
## Execution provider

Select via `execution_provider` in config or `MEMEX_EXECUTION_PROVIDER`:

| Provider | Platforms | Notes |
|----------|-----------|-------|
| auto | all | Default. Uses the CPU on every platform |
| cpu | all | Force CPU execution |
| coreml | macOS | Uses CoreML in float32; `compute_units` controls ane/gpu/cpu/all. Compiling the model takes several times as long as loading it on the CPU |
| cuda | Linux/NVIDIA | Requires a binary built with `--features cuda` and CUDA 12/cuDNN runtime libraries |

When `execution_provider = "cuda"`, you can optionally select a GPU with
`cuda_device_id` or `MEMEX_CUDA_DEVICE_ID`.

When loading CUDA, memex first tries the system loader paths, then any
configured `cuda_library_paths` / `cudnn_library_paths`, then common CUDA install
locations and active `venv` / `conda` `site-packages/nvidia/*/lib` directories.
If your system keeps CUDA or cuDNN in a nonstandard location, set
`MEMEX_CUDA_LIBRARY_PATHS` and `MEMEX_CUDNN_LIBRARY_PATHS` or the matching config
keys.
## Config (optional)

Create `~/.memex/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true  # true or "local", "remote", false
auto_index_on_search = true
include_reasoning = false  # opt in to plaintext reasoning; encrypted/redacted payloads stay excluded
token_usage = false  # opt in to local token and cost tracking
model = "minilm"  # local: minilm, bge, nomic, gemma, potion, or a fastembed model name; remote: API model name
embedding_batch_size = 64  # local and remote
# embedding_dimensions = 512  # remote only
# embedding_base_url = "https://api.openai.com/v1"  # remote only
# embedding_api_key_env = "OPENAI_API_KEY"  # remote only; default MEMEX_EMBEDDING_API_KEY, then OPENAI_API_KEY
# embedding_api_key = "sk-..."  # remote only; takes precedence over embedding_api_key_env
embedding_timeout_secs = 60  # remote only; 1 to 600
embedding_max_retries = 3  # remote only; 0 to 10
execution_provider = "auto"  # auto, cpu, coreml, cuda
cuda_device_id = 0  # optional, when execution_provider = "cuda"
cuda_library_paths = ["/usr/local/cuda/lib64"]  # optional list of CUDA library dirs
cudnn_library_paths = ["/usr/lib/x86_64-linux-gnu"]  # optional list of cuDNN library dirs
compute_units = "ane"  # CoreML only: ane, gpu, cpu, all
index_freshness = "bounded"  # strict, bounded or none
scan_cache_ttl = 3600  # seconds (default 1 hour)
max_indexed_tool_input_bytes = 65536  # 64 KiB default
max_indexed_tool_output_bytes = 262144  # 256 KiB default
exclude_paths = ["~/.claude/projects/*-client-*", "~/work/**"]  # never index matched transcripts
claude_paths = ["~/.claude", "~/.claude-profiles/*"]  # Claude config dirs to read; globs allowed
index_service_mode = "interval"  # interval or continuous
index_service_interval = 3600  # seconds (ignored when mode = "continuous")
index_service_poll_interval = 30  # seconds
index_service_web_ui = false  # serve local browser; forces continuous mode when true
index_service_mcp = false  # serve MCP from the daemon; forces continuous mode when true
index_service_web_listen = "127.0.0.1:6363"
index_service_label = "memex-index"  # service name (default: com.memex.index on macOS)
index_service_systemd_dir = "~/.config/systemd/user"  # Linux only
claude_resume_cmd = "claude --resume {session_id}"
codex_resume_cmd = "codex resume {session_id}"
cursor_resume_cmd = "cursor-agent --resume {session_id}"
opencode_resume_cmd = "opencode --session {session_id}"
pi_resume_cmd = "pi --session {source_path_shell}"
# copilot_resume_cmd = "your-copilot-resume-command {session_id}"
grok_resume_cmd = "cd {cwd_shell} && grok --resume {session_id}"
bob_resume_cmd = "cd {cwd_shell} && bob --resume {session_id}"
jcode_resume_cmd = "cd {cwd_shell} && jcode --resume {session_id}"
muse_resume_cmd = "cd {cwd_shell} && muse resume {session_id}"
kimi_resume_cmd = "cd {cwd_shell} && kimi --session {session_id}"
goose_resume_cmd = "cd {cwd_shell} && goose session --resume --session-id {session_id}"
herdr_resume = "tab"  # inside a herdr pane: "tab" (default), "split", or "off"; an already-open session is focused instead
rex_resume = "tab"    # inside a Rex terminal: "tab" (default), "split", or "off"; an already-open session is focused instead

[mcp]
listen = "127.0.0.1:5363"
allowed_hosts = []
allowed_origins = []
# public_url = "https://memex.example.com"

[dream]
enabled = false  # opt in to dreaming; see dreaming.md for every key
auto = false  # with enabled, let the daemon start dream runs
# executor = "claude"  # "claude" or "codex"; unset picks whichever is on PATH
```

Daemon logs and the plist live under `~/.memex` by default (macOS). On Linux, systemd units are created in `~/.config/systemd/user/`.

`index_freshness` sets how far reads may trail the transcripts on disk. A search first takes in
what the file-system journal names as changed (macOS), and a session read first takes in what its
own transcripts gained, so a live session reads back its newest messages. `bounded` (default)
walks the source trees only when that cannot name the changes and the last walk is older than
`scan_cache_ttl`, and a session read waits at most a second for an ingest that holds the index
before it reads the index as it stands, with a warning; `strict` walks whenever the changes
cannot be named and fails a read whose refresh fails, so every read sees the index as of the
moment it started; `none` serves the index as it stands. Leaving it unset with
`auto_index_on_search = false` means `none`. The TUI's manual refresh (`i`) always runs, as
`strict`.
`include_reasoning` defaults to false. Set it to true (or pass `memex index
--include-reasoning`) to add plaintext reasoning as BM25-only records. Encrypted
and redacted reasoning payloads are always excluded.
`max_indexed_tool_*_bytes` limits oversized tool payloads while leaving user and assistant text
unchanged. memex keeps roughly the first three quarters and final quarter, with a marker reporting
the omitted middle. Each value must be at least 1024 bytes. Run `memex index rebuild` to apply
new limits to records that are already indexed.
`exclude_paths` takes glob patterns matched against transcript source paths at index time, so
matched transcripts never enter the index (a leading `~/` is expanded to your home directory).
Adding a pattern also removes records previously indexed from matched paths — no rebuild
required. For one-off runs, pass `--exclude GLOB` (repeatable) to `memex index`.
`claude_paths` lists the Claude Code config directories (or their `projects` directories) to
index and scan for token usage, for one config directory per account selected with
`CLAUDE_CONFIG_DIR`. It replaces `CLAUDE_CONFIG_DIR` and the default `~/.claude/projects` and
`~/.config/claude/projects`; `--claude-path` overrides it for one run. A leading `~/` expands
to your home directory. Each glob component matches one directory level (`**` is rejected)
and is re-expanded on every full scan, so the daemon's next resync picks up a profile created
later without a restart. `memex sessions --profile NAME` lists the sessions of one config
directory: `NAME` is the directory's name, such as `work` for `~/.claude-profiles/work` or
`.claude` for `~/.claude`. Moving transcripts between configured roots retires the records
indexed under their old paths and indexes them again from the new ones; see
[vector-migration.md](vector-migration.md) to keep their embeddings.
`execution_provider` applies to local ONNX-backed models; `potion` uses the model2vec backend.
`cuda_library_paths` and `cudnn_library_paths` accept path lists and are only used
when `execution_provider = "cuda"`.

Resume command templates accept `{session_id}`, `{project}`, `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`, plus shell-quoted `{source_path_shell}`, `{source_dir_shell}`, and `{cwd_shell}`. `{claude_env}` expands to `CLAUDE_CONFIG_DIR='<dir>' ` for a local Claude session read from a configured profile, or `env -u CLAUDE_CONFIG_DIR ` for the default `~/.claude` and `~/.config/claude` directories so another account's inherited setting cannot select the wrong profile. Remote sessions and unrecognized roots leave it empty; the default Claude template uses it.

Workflow agent transcripts may have overlapping direct and workflow copies. Indexing merges
their unique events into one session, and token usage counts each shared response once, so
resuming a tail-only copy preserves the history that exists only in the workflow copy.

The skill definitions are bundled in `skills/`.
