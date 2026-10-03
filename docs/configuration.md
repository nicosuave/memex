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
memory documents to the endpoint. Plain `http://` to a non-loopback host is rejected when a
key is set and warned about otherwise; `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` are
honored.
## Execution provider

Select via `execution_provider` in config or `MEMEX_EXECUTION_PROVIDER`:

| Provider | Platforms | Notes |
|----------|-----------|-------|
| auto | all | Default. Uses CoreML on macOS, CPU elsewhere |
| cpu | all | Force CPU execution |
| coreml | macOS | Uses CoreML; `compute_units` controls ane/gpu/cpu/all |
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
scan_cache_ttl = 3600  # seconds (default 1 hour)
max_indexed_tool_input_bytes = 65536  # 64 KiB default
max_indexed_tool_output_bytes = 262144  # 256 KiB default
exclude_paths = ["~/.claude/projects/*-client-*", "~/work/**"]  # never index matched transcripts
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
herdr_resume = "tab"  # inside a herdr pane: "tab" (default), "split", or "off"

[mcp]
listen = "127.0.0.1:5363"
allowed_hosts = []
allowed_origins = []
# public_url = "https://memex.example.com"
```

Daemon logs and the plist live under `~/.memex` by default (macOS). On Linux, systemd units are created in `~/.config/systemd/user/`.

`scan_cache_ttl` controls how long auto-indexing considers scans fresh.
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
`execution_provider` applies to local ONNX-backed models; `potion` uses the model2vec backend.
`cuda_library_paths` and `cudnn_library_paths` accept path lists and are only used
when `execution_provider = "cuda"`.

Resume command templates accept `{session_id}`, `{project}`, `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`, plus shell-quoted `{source_path_shell}`, `{source_dir_shell}`, and `{cwd_shell}`.

The skill definitions are bundled in `skills/`.
