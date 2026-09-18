# Installation and updates

[Back to Memex](../README.md)

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

### Cargo (prebuilt binary via binstall)

```bash
cargo install cargo-binstall
cargo binstall --locked --git https://github.com/nicosuave/memex
```

Prebuilt for macOS arm64 and Linux x86_64/arm64; other targets fall back to compiling from source.

### Cargo (Build from source)

```bash
# On Ubuntu/Debian, install build dependencies first:
sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config
cargo install --locked --git https://github.com/nicosuave/memex
```

### Native Mac App

```
brew install nicosuave/tap/memex-app
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
          daemon.enable = true;
          settings = {
            index_service_mode = "continuous";
            embeddings = true;
            include_reasoning = false;
            model = "minilm";
            execution_provider = "auto"; # coreml on macOS, cpu elsewhere
            cuda_device_id = 0; # optional when execution_provider = "cuda"
            cuda_library_paths = ["/usr/local/cuda/lib64"]; # optional override
            cudnn_library_paths = ["/usr/lib/x86_64-linux-gnu"]; # optional override
            compute_units = "ane"; # CoreML only: ane, gpu, cpu, all
            auto_index_on_search = true;
            token_usage = false; # opt in to local token and cost tracking
            index_service_interval = 3600;
          };
        };
      }
    ];
  };
}
```

</details>

Then install the shared skill used by Codex, OpenCode, Pi, and Oh My Pi:

```bash
memex skill install --target shared
```

The shared `memex-search` skill is installed once at
`~/.agents/skills/memex-search/SKILL.md` for Codex, OpenCode, Pi, and Oh My Pi.
For Claude Code, use `memex skill install --target claude`; its copy lives at
`~/.claude/skills/memex-search/SKILL.md`.

Launching `memex` in a human terminal offers an update when a newer release is
available. Press Enter to update Memex and refresh its installed skill copies, or
choose no to continue into the TUI. Homebrew installs run `brew update` followed by
`brew upgrade nicosuave/tap/memex`; skills are refreshed by the newly installed binary.
After updating, run `memex` again to start that version.

Enabled daemons follow binary upgrades automatically between indexing passes.
Homebrew registrations use `opt/memex/bin/memex`, so direct `brew upgrade
nicosuave/tap/memex` works without a second service manager. Cargo/manual installs
follow replacement at their registered executable path; replace binaries atomically.
Nix profile registrations follow the profile link. A running indexing pass finishes
before handoff; Web UI and MCP connections reconnect when the process reloads.

**One-time migration:** daemons installed before this behavior need
`memex daemon restart` from the newly installed binary. Use `--root <path>` for
each custom data root. This also replaces old version-specific Homebrew service
paths. After migration, interval services use the new binary on their next run;
continuous daemons detect a replacement within a few seconds when idle.

`memex update` also runs `memex daemon reconcile` using the installed binary.
Reconciliation updates an existing enabled, loaded Memex registration and checks
continuous-daemon readiness; absent, stopped, disabled, and Nix-owned registrations
stay unchanged. It preserves the registered arguments, environment and schedule.
Use `memex daemon reconcile --root <path>` for a custom root. `memex daemon status`
shows the running version, PID, executable, readiness and whether its file identity
matches the installed executable, including same-version rebuilds. Older daemons
without runtime reporting show their running build as unavailable.

For Nix-managed services, update the flake input and activate with
`home-manager switch` or `nixos-rebuild switch`. Home Manager's
`programs.memex.daemon.enable` owns the native Linux/macOS service; Linux automatic
activation requires `systemd.user.startServices = "sd-switch"` (or `true`).
An explicit `false`/`"suggest"` setting leaves service activation manual.
The NixOS module restarts active owned services on switch, preserves stopped
services, and handles interval/continuous transitions. Disable
`services.memex.enable` and switch before removing the module import.
Disable a CLI-owned registration before enabling a declarative service; choose
one owner. `memex update` refuses to overwrite immutable Nix store binaries.
For a standalone Nix profile, upgrade the selected profile package; an already
migrated daemon follows its stable profile link, or run `memex daemon reconcile`
through that profile to activate an existing registration explicitly.

Agents and scripts still receive update notices but never a startup prompt. Memex
recognizes CI, Codex, and Claude Code environment markers; use `--non-interactive`
explicitly for other agents, including those using a PTY. Bare noninteractive
`memex` prints help, while `search` and other data commands run normally. To update:

```bash
memex update --yes
```

Without `--yes`, explicit `memex update` requires a human terminal and confirmation.
Both update paths replace existing shared/Claude skill copies, including local edits;
they do not install missing copies. Restart your agent after its skill is updated.

Searches warn on stderr when an installed skill differs from the running binary
(outdated or locally modified). `memex skill status` shows which copies differ;
`memex skill update` refreshes skills without upgrading Memex. Searches never update
anything or prompt, and JSONL/TOON output on stdout stays clean. Use
`memex skill cleanup` to explicitly remove obsolete paths from older releases.

Release checks have a two-second network timeout and are cached for six hours
(failed checks retry after five minutes). `--no-update-check` skips release checks
without hiding stale-skill warnings. Install never overwrites a differing skill file.
## Build from source

```
cargo build --release
```

Linux with NVIDIA CUDA support:

```
cargo build --release --features cuda
```

Binary:
```
./target/release/memex
```
## Setup (manual)

If you built from source, install the skill embedded in that build:

```bash
memex skill install --target shared
```

Omit `--target` for an interactive menu of detected Claude/Codex/OpenCode/Pi/Oh My Pi installations.
