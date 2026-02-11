# memex

Fast local history search for Claude, Codex CLI & OpenCode logs. Uses BM-25 and optionally embeds your transcripts locally for hybrid search.

Mostly intended for agents to use via skill. The intended workflow is to ask agent about a previous session & then the agent can narrow things down & retrieve history as needed.

Includes a TUI for browsing, finding and resuming both agent CLI sessions.

![memex tui](docs/tui.png?raw=1&v=2)

## Install
```bash
brew install nicosuave/tap/memex
```

Or

```bash
curl -fsSL https://raw.githubusercontent.com/nicosuave/memex/main/scripts/setup.sh | sh
```

Or (from the [AUR](https://aur.archlinux.org/packages/memex) on Arch Linux):

```bash
paru -S memex
```

Or (with [Nix](https://nixos.org/)):

```bash
nix run github:nicosuave/memex
```

<details>
<summary>Nix development and advanced configuration</summary>

**Development shell:**

```bash
nix develop
```

> **Note:** No binary cache is configured, so first builds compile from source.

**NixOS service:**

Enable background indexing with the provided module:

```nix
{
  inputs.memex.url = "github:nicosuave/memex";

  outputs = { nixpkgs, memex, ... }: {
    nixosConfigurations.default = nixpkgs.lib.nixosSystem {
      modules = [
        memex.nixosModules.default
        {
          services.memex = {
            enable = true;
            continuous = true; # Run as a daemon (optional)
          };
        }
      ];
    };
  };
}
```

**Home Manager:**

Configure memex declaratively (generates `~/.memex/config.toml`):

```nix
{
  inputs.memex.url = "github:nicosuave/memex";

  outputs = { memex, ... }: {
    # Inside your Home Manager configuration
    modules = [
      memex.homeManagerModules.default
      {
        programs.memex = {
          enable = true;
          settings = {
            embeddings = true;
            model = "minilm";
            compute_units = "ane"; # macOS: ane, gpu, cpu, all
            auto_index_on_search = true;
            index_service_interval = 3600;
          };
        };
      }
    ];
  };
}
```

</details>

Then run setup to install the skills:

```bash
memex setup
```

Restart Claude/Codex after setup.

## Quickstart

Index (incremental):
```
memex index
```

Search (JSONL default):
```
memex search "your query" --limit 20
```

TUI:
```
memex tui
```

Notes:
- Embeddings are enabled by default.
- Searches run an incremental reindex by default (configurable).

Full transcript:
```
memex session <session_id>
```

Single record:
```
memex show <doc_id>
```

Human output:
```
memex search "your query" -v
```

## Build from source

```
cargo build --release
```

Binary:
```
./target/release/memex
```

## Setup (manual)

If you built from source, run setup to install:

```bash
memex setup
```

This detects which tools are installed (Claude/Codex) and presents an interactive menu to select which to configure.
## Search modes

| Need | Command |
| --- | --- |
| Exact terms | `search "exact term"` |
| Fuzzy concepts | `search "concept" --semantic` |
| Mixed | `search "term concept" --hybrid` |

## Common filters

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>`
- `--source claude|codex`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--limit <n>`
- `--min-score <float>`
- `--sort score|ts`
- `--top-n-per-session <n>`
- `--unique-session`
- `--fields score,ts,doc_id,session_id,snippet`
- `--json-array`

## Background index service

Works on macOS (launchd) and Linux (systemd).

Enable:
```
memex index-service enable
memex index-service enable --continuous
```

Disable:
```
memex index-service disable
```

`index-service` reads config defaults (mode, interval, log paths). Flags override.

On Linux, creates systemd user units in `~/.config/systemd/user/`. On macOS, creates a launchd plist in `~/.memex/`.

## Embeddings

Disable:
```
memex index --no-embeddings
```

Recommended when embeddings are on (especially non-`potion` models): run the background
index service or `index --watch`, and consider setting `auto_index_on_search = false`
to keep searches fast.

## Embedding model

Select via `--model` flag or `MEMEX_MODEL` env var:

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

## Config (optional)

Create `~/.memex/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true
auto_index_on_search = true
model = "minilm"  # minilm, bge, nomic, gemma, potion
compute_units = "ane"  # macOS only: ane, gpu, cpu, all
scan_cache_ttl = 3600  # seconds (default 1 hour)
index_service_mode = "interval"  # interval or continuous
index_service_interval = 3600  # seconds (ignored when mode = "continuous")
index_service_poll_interval = 30  # seconds
index_service_label = "memex-index"  # service name (default: com.memex.index on macOS)
index_service_systemd_dir = "~/.config/systemd/user"  # Linux only
claude_resume_cmd = "claude --resume {session_id}"
codex_resume_cmd = "codex resume {session_id}"
```

Service logs and the plist live under `~/.memex` by default (macOS). On Linux, systemd units are created in `~/.config/systemd/user/`.

`scan_cache_ttl` controls how long auto-indexing considers scans fresh.

Resume command templates accept `{session_id}`, `{project}`, `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`.

The skill definitions are bundled in `skills/`.
