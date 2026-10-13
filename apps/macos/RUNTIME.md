# Optional agent runtime builds

History browsing, search, workspace tools and external resume remain buildable
without private source. `MEMEX_HISTORY_ONLY=1` explicitly selects that graph even
when a local runtime path has previously been configured. Public releases
(the GitHub release asset and the `memex-app` Homebrew cask) are always
history-only and never contain the runtime.

The runtime-enabled app consumes `SQACP`, `SQACPHost`, `SQACPUI`, `SQMcpApps`,
`SQMcpAppsUI`, a Rust static
archive and the compiled Claude SDK helper from one source checkout. The private
repository and full immutable revision are recorded in
[`runtime-source.json`](runtime-source.json). Obtain that checkout through your
normal authorized Git access. These scripts do not clone private source, install
toolchains, or publish compiled artifacts.

## Prepare the pinned runtime

Use a clean checkout at the locked revision. Install Xcode command-line tools,
Rust with the desired Apple target, Bun, and jq through your normal development
setup. Boxington is used when installed; an uncached Cargo fallback is reported.

```sh
export MEMEX_AGENT_RUNTIME_ROOT=/absolute/path/to/authorized/runtime-checkout
export RUSTUP_TOOLCHAIN=stable
ARCHES=arm64 bash apps/macos/scripts/prepare-runtime.sh
export MEMEX_AGENT_RUNTIME_LIBRARY="$PWD/apps/macos/.build/runtime/arm64/libsq_acp_runtime.a"

swift build --package-path apps/macos --product Memex --force-resolved-versions
swift build --package-path apps/macos --product MemexExecutionHost --force-resolved-versions
swift test --package-path apps/macos --force-resolved-versions --no-parallel
```

When a runtime checkout is configured (`MEMEX_AGENT_RUNTIME_ROOT` or the ignored
`apps/macos/.local-runtime-root`), the package manifest requires
`MEMEX_AGENT_RUNTIME_LIBRARY` to name the prepared archive and stops otherwise.
It never falls back to an unverified archive inside the runtime checkout.
`build.sh` sets the variable from verified provenance. Set `MEMEX_HISTORY_ONLY=1`
to build without the runtime.

Preparation always invokes the selected checkout's Rust build with `--locked`
and the helper's Bun install with `--frozen-lockfile`, then compiles the helper.
It does not accept an archive copied from another checkout. The default minimum
deployment target is macOS 14. Set `ARCHES="arm64 x86_64"` for both slices; install
the corresponding Rust targets first. Swift and the helper must support the
same requested architectures. The normal packaged distribution remains arm64.

`RUSTUP_TOOLCHAIN` explicitly selects an installed rustup toolchain for both Cargo
and compiler provenance. Preparation inspects every archive object and helper's
Mach-O minimum OS. A Homebrew Rust standard library built for a newer macOS will
be rejected even when `MACOSX_DEPLOYMENT_TARGET=14.0` was set for this crate;
select a compatible rustup toolchain instead of ignoring linker warnings.

Generated artifacts and `runtime-provenance.json` live in the ignored
`apps/macos/.build/runtime/<sorted-architectures>/` directory. `MEMEX_RUNTIME_OUTPUT`
can select another generated-output directory. `CARGO_TARGET_DIR` may select an
existing isolated Cargo build directory; Cargo still validates the source and
lockfile before any output is qualified.

With `MBX_VERIFY=1`, preparation requires Boxington's per-architecture JSON report
and rejects any divergence, even when the compiler exits successfully. Reports
are retained under the output's `slices` directory. A caller's `MBX_STATS_REPORT`
also receives the most recent architecture's report. Cold builds may verify zero
cached compilations; a cache qualification must additionally establish a positive
verification count using a warm cache and a fresh target directory. Configured
alternative compiler wrappers retain their own behavior and reporting contract.

## Package and inspect provenance

```sh
ARCHES=arm64 MEMEX_CLI=/absolute/path/to/this-checkouts/memex \
  bash apps/macos/scripts/build.sh release
```

`build.sh` verifies the prepared runtime before and after Swift compilation. A
source edit, revision change, modified lock, wrong architecture, or changed
archive/helper hash fails packaging with an instruction to prepare again. Old
`MEMEX_AGENT_RUNTIME_LIBRARY` or `MEMEX_CLAUDE_HELPER` overrides must be removed or
match the verified generated outputs exactly.

A runtime-enabled app bundles and signs both `MemexExecutionHost` and the Claude
helper. The execution host is signed without entitlements. The Bun-compiled
Claude helper is signed with `bundle/ClaudeHelper.entitlements`, Bun's documented
JIT entitlements for compiled executables; neither helper receives the app's
Apple Events or microphone entitlements. History-only bundles omit both helpers
and the provenance file, because the app never launches the execution host and
it could only report `runtime_unavailable`. `build.sh` prints the runtime mode
and checks the bundle contents against it.

A runtime-enabled bundle's
signed Resources directory contains a sanitized provenance file: immutable
source revision, source fingerprint, dirty/development state, dependency lock
hashes, compiler versions, architecture slices, compiled-input hashes and signed
helper hashes. Workstation paths are excluded. Hashes of compiled inputs and
signed helpers are distinct because code signing changes Mach-O bytes.

Verification without compilation is available with the same source, output,
architecture and mode options:

```sh
ARCHES=arm64 bash apps/macos/scripts/prepare-runtime.sh --verify
```

## Explicit development builds

When developing the runtime alongside Memex, opt in to uncommitted source:

```sh
ARCHES=arm64 bash apps/macos/scripts/prepare-runtime.sh --development
ARCHES=arm64 MEMEX_RUNTIME_DEVELOPMENT=1 MEMEX_CLI=/absolute/path/to/memex \
  bash apps/macos/scripts/build.sh debug
```

The fingerprint includes staged and unstaged diffs and nonignored untracked file
contents. Source changes during preparation or Swift compilation invalidate the
result. Dirty initialized submodules are rejected; generated ignored outputs do
not change the fingerprint. An app update that alters the pinned runtime lock
requires preparation again even if the old artifact still exists.

Development and candidate builds are ad-hoc local/CI artifacts. Developer ID
packaging rejects both modes and requires the clean locked source. No preparation
or verification command signs, notarizes, uploads or creates a release.

## Release builds

`apps/macos/scripts/build-release.sh` requires an explicit runtime mode and
fails when `MEMEX_RELEASE_RUNTIME` is unset:

- `MEMEX_RELEASE_RUNTIME=history-only` builds without the runtime, even when
  `MEMEX_AGENT_RUNTIME_ROOT` or `.local-runtime-root` is configured. Public
  releases use this mode; `scripts/release_macos_local.sh` always selects it and
  rejects any other value.
- `MEMEX_RELEASE_RUNTIME=locked` requires a configured runtime checkout at the
  clean pinned revision and rejects history-only, development and candidate
  settings. It is not published to the public cask.

After building, the script checks that `runtime-provenance.json`,
`claude-agent-sdk-host` and `MemexExecutionHost` are present exactly when the
mode includes the runtime, and prints the mode.

Developer ID signing and locked release builds also pass `--require-published`
to `prepare-runtime.sh`. The pinned revision must be an ancestor of the runtime
checkout's fetched `origin/main`. A pinned pull-request head that has not merged,
or was squash-merged and orphaned, fails packaging. The check never fetches; run
`git fetch origin` in the runtime checkout first. Public CI has no runtime
source and does not run it.

## Runtime integration CI

Public Memex CI retains the history-only Swift build/test graph and runs the
runtime provenance regression script without private source. The private runtime
repository owns `.github/workflows/memex-runtime.yml`. It runs on relevant pull
requests and main changes, reads an immutable public Memex revision from
`ci/memex-consumer.json`, builds the current clean runtime candidate, compiles both
native products, tests the complete runtime-enabled Swift package, and validates
an ad-hoc app bundle. It does not upload private source or binaries.

Candidate CI uses `--candidate-revision FULL_SHA` and
`MEMEX_RUNTIME_CANDIDATE_REVISION=FULL_SHA` to explicitly qualify a clean runtime
revision newer than the consumer's production lock. It cannot accept dirty
source and cannot produce a Developer ID release bundle. This breaks the
otherwise circular consumer/runtime pin update without weakening release checks.

For a coordinated update: commit the Memex consumer, update the private consumer
pin to that full SHA, commit the runtime change and run its integration CI, then
update Memex's runtime lock to the resulting immutable runtime commit. Re-prepare
the locked artifacts and run the runtime-enabled consumer checks before delivery.
