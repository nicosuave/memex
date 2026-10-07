# Optional agent runtime builds

History browsing, search, workspace tools and external resume remain buildable
without private source. `MEMEX_HISTORY_ONLY=1` explicitly selects that graph even
when a local runtime path has previously been configured.

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

The app bundles and signs both `MemexExecutionHost` and the Claude helper. Its
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
