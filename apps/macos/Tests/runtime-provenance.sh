#!/usr/bin/env bash
# Exercise provenance policy with real Git/files/hashes and fake compilers.
# No private source, macOS services, network calls or actual native builds.
set -euo pipefail
APP_ROOT=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/app/scripts" "$scratch/bin" "$scratch/source/packages/sq-acp/rust/sq-acp-ffi-runtime" \
  "$scratch/source/packages/sq-acp/Tools/ClaudeAgentSDKHost/src"
cp "$APP_ROOT/scripts/prepare-runtime.sh" "$scratch/app/scripts/"
manifest="$scratch/source/packages/sq-acp/rust/sq-acp-ffi-runtime"
helper="$scratch/source/packages/sq-acp/Tools/ClaudeAgentSDKHost"
printf '[package]\nname = "fixture"\nversion = "0.1.0"\n' > "$manifest/Cargo.toml"
printf 'fixture Cargo lock\n' > "$manifest/Cargo.lock"
printf 'fixture Bun lock\n' > "$helper/bun.lock"
printf 'fixture source\n' > "$helper/src/index.ts"
git -C "$scratch/source" init -q
git -C "$scratch/source" add .
git -C "$scratch/source" -c user.name=Test -c user.email=test@example.invalid commit -qm fixture
revision=$(git -C "$scratch/source" rev-parse HEAD)
jq -n --arg revision "$revision" '{version:1,repository:"sidequery/mono",revision:$revision}' > "$scratch/app/runtime-source.json"

cat > "$scratch/bin/uname" <<'STUB'
#!/usr/bin/env bash
case "${1:-}" in -s) echo Darwin ;; -m) echo arm64 ;; *) exit 1 ;; esac
STUB
cat > "$scratch/bin/cargo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" != +* ]] || { echo 'no such command: toolchain selector' >&2; exit 1; }
if [[ -n "${EXPECTED_CARGO:-}" ]]; then
  [[ "$0" == "$EXPECTED_CARGO" && "$RUSTC" == "$EXPECTED_RUSTC" ]] || exit 1
  [[ "${RUSTC_WRAPPER:-}" == "${EXPECTED_WRAPPER:-}" ]] || exit 1
fi
target=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == --target ]]; then target=$2; shift; fi
  shift
done
if [[ "${FIXTURE_EXPECTED_TARGET+x}" == x ]]; then
  [[ "$target" == "$FIXTURE_EXPECTED_TARGET" ]] || {
    echo "Expected target '$FIXTURE_EXPECTED_TARGET', got '$target'" >&2; exit 1;
  }
fi
target="${target:-${CARGO_BUILD_TARGET:-${FIXTURE_CONFIG_TARGET:-}}}"
mkdir -p "$CARGO_TARGET_DIR${target:+/$target}/release"
printf 'compiled library fixture %s\n' "${target:-native}" > "$CARGO_TARGET_DIR${target:+/$target}/release/libsq_acp_runtime.a"
STUB
cat > "$scratch/bin/mbx" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
cargo "$@"
if [[ -n "${MBX_STATS_REPORT:-}" && "${FIXTURE_MISSING_REPORT:-0}" != 1 ]]; then
  divergences=${FIXTURE_DIVERGENCES:-0}
  if [[ -n "${FIXTURE_DIVERGENT_TARGET:-}" && "$*" == *"$FIXTURE_DIVERGENT_TARGET"* ]]; then divergences=1; fi
  printf '{"verifications":6,"divergences":%s}\n' "$divergences" > "$MBX_STATS_REPORT"
fi
STUB
cat > "$scratch/bin/bun" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  --version) echo fixture ;;
  install) [[ "$2" == --frozen-lockfile ]] ;;
  build)
    output=""
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == --outfile ]]; then output=$2; shift; fi
      shift
    done
    [[ -n "$output" ]]
    cat src/index.ts > "$output"
    [[ "${MUTATE_DURING_BUILD:-0}" != 1 ]] || printf 'changed mid-build\n' >> src/index.ts
    ;;
  *) exit 1 ;;
esac
STUB
cat > "$scratch/bin/rustc" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == -vV ]]; then
  printf 'host: %s\n' "${FIXTURE_RUST_HOST-aarch64-apple-darwin}"
else
  echo fixture
fi
STUB
printf '#!/usr/bin/env bash\necho 26.0\n' > "$scratch/bin/xcrun"
cat > "$scratch/bin/lipo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == -create ]]; then
  shift
  inputs=()
  while [[ "$1" != -output ]]; do inputs+=("$1"); shift; done
  cat "${inputs[@]}" > "$2"
fi
STUB
cat > "$scratch/bin/otool" <<'STUB'
#!/usr/bin/env bash
printf 'fixture.o:\n cmd LC_BUILD_VERSION\n minos %s\n' "${FIXTURE_MINIMUM_OS:-14.0}"
STUB
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH"
export ARCHES=arm64
export MEMEX_AGENT_RUNTIME_ROOT="$scratch/source"
export MEMEX_RUNTIME_OUTPUT="$scratch/output"
export CARGO_TARGET_DIR="$scratch/cargo"
unset MEMEX_AGENT_RUNTIME_LIBRARY MEMEX_CLAUDE_HELPER
unset RUSTUP_TOOLCHAIN
unset CARGO_BUILD_TARGET MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION
prepare="$scratch/app/scripts/prepare-runtime.sh"

expect_failure() {
  local message=$1
  shift
  if "$@" > "$scratch/failure" 2>&1; then
    echo "Unexpected success: $*" >&2; exit 1
  fi
  grep -Fq "$message" "$scratch/failure" || { cat "$scratch/failure"; exit 1; }
}

expect_failure 'output is missing' bash "$prepare" --verify
env FIXTURE_EXPECTED_TARGET=aarch64-apple-darwin bash "$prepare"
grep -Fxq 'compiled library fixture aarch64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
bash "$prepare" --verify
env ARCHES=x86_64 FIXTURE_EXPECTED_TARGET=x86_64-apple-darwin bash "$prepare"
grep -Fxq 'compiled library fixture x86_64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
env ARCHES=x86_64 FIXTURE_RUST_HOST=x86_64-apple-darwin FIXTURE_EXPECTED_TARGET=x86_64-apple-darwin bash "$prepare"
grep -Fxq 'compiled library fixture x86_64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
env FIXTURE_RUST_HOST=x86_64-apple-darwin FIXTURE_EXPECTED_TARGET=aarch64-apple-darwin bash "$prepare"
grep -Fxq 'compiled library fixture aarch64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
env ARCHES='arm64 x86_64' MBX_VERIFY=1 bash "$prepare"
grep -Fxq 'compiled library fixture aarch64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
grep -Fxq 'compiled library fixture x86_64-apple-darwin' "$scratch/output/libsq_acp_runtime.a"
env ARCHES='arm64 x86_64' bash "$prepare" --verify
# Only native-host qualification opts out of --target, with fresh build state.
env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 MBX_VERIFY=1 FIXTURE_EXPECTED_TARGET= \
  CARGO_TARGET_DIR="$scratch/native-arm64" bash "$prepare"
grep -Fxq 'compiled library fixture native' "$scratch/output/libsq_acp_runtime.a"
bash "$prepare" --verify
env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 ARCHES=x86_64 FIXTURE_RUST_HOST=x86_64-apple-darwin \
  FIXTURE_EXPECTED_TARGET= CARGO_TARGET_DIR="$scratch/native-x86" bash "$prepare"
grep -Fxq 'compiled library fixture native' "$scratch/output/libsq_acp_runtime.a"
expect_failure 'fresh Cargo target directory' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
  CARGO_TARGET_DIR="$scratch/native-arm64" bash "$prepare"
expect_failure 'exactly the Rust compiler' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
  ARCHES=x86_64 bash "$prepare"
expect_failure 'exactly the Rust compiler' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
  ARCHES='arm64 x86_64' bash "$prepare"
expect_failure 'supported Apple host target' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
  FIXTURE_RUST_HOST= bash "$prepare"
for target_setting in CARGO_BUILD_TARGET FIXTURE_CONFIG_TARGET; do
  expect_failure 'produced no native archive' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
    "$target_setting=aarch64-apple-darwin" CARGO_TARGET_DIR="$scratch/default-$target_setting" \
    MEMEX_RUNTIME_OUTPUT="$scratch/default-output-$target_setting" bash "$prepare"
  [[ ! -e "$scratch/default-output-$target_setting/runtime-provenance.json" ]]
done
expect_failure 'Boxington verification failed' env MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 \
  MBX_VERIFY=1 FIXTURE_DIVERGENCES=1 CARGO_TARGET_DIR="$scratch/native-divergence" \
  MEMEX_RUNTIME_OUTPUT="$scratch/native-divergence-output" bash "$prepare"
[[ ! -e "$scratch/native-divergence-output/runtime-provenance.json" ]]
bash "$prepare"
expect_failure 'exceeds the deployment target' env FIXTURE_MINIMUM_OS=27.0 bash "$prepare" --verify
jq -e --arg revision "$revision" '.mode == "locked" and .source.revision == $revision and .source.dirty == false' \
  "$scratch/output/runtime-provenance.json" >/dev/null

printf 'corrupt archive\n' >> "$scratch/output/libsq_acp_runtime.a"
expect_failure 'provenance mismatch' bash "$prepare" --verify
bash "$prepare"
printf 'corrupt helper\n' >> "$scratch/output/claude-agent-sdk-host"
expect_failure 'provenance mismatch' bash "$prepare" --verify
bash "$prepare"

printf 'uncommitted\n' >> "$helper/src/index.ts"
expect_failure 'clean source checkout' bash "$prepare"
expect_failure 'clean source checkout' bash "$prepare" --verify
bash "$prepare" --development
bash "$prepare" --verify --development
jq -e '.mode == "development" and .source.dirty == true' "$scratch/output/runtime-provenance.json" >/dev/null
printf 'untracked input\n' > "$scratch/source/new-input"
expect_failure 'provenance mismatch' bash "$prepare" --verify --development
bash "$prepare" --development
printf 'updated untracked input\n' > "$scratch/source/new-input"
expect_failure 'provenance mismatch' bash "$prepare" --verify --development
expect_failure 'changed during compilation' env MUTATE_DURING_BUILD=1 bash "$prepare" --development
expect_failure 'clean source checkout' bash "$prepare" --candidate-revision "$revision"

git -C "$scratch/source" add .
git -C "$scratch/source" -c user.name=Test -c user.email=test@example.invalid commit -qm candidate
candidate=$(git -C "$scratch/source" rev-parse HEAD)
expect_failure 'does not match runtime-source.json' bash "$prepare"
bash "$prepare" --candidate-revision "$candidate"
bash "$prepare" --verify --candidate-revision "$candidate"
expect_failure 'full immutable SHA' bash "$prepare" --candidate-revision main
jq -n --arg revision "$candidate" '{version:1,repository:"sidequery/mono",revision:$revision}' > "$scratch/app/runtime-source.json"
expect_failure 'provenance mismatch' bash "$prepare" --verify --candidate-revision "$candidate"
bash "$prepare"
bash "$prepare" --verify

# Distributed builds require the pin to survive on the runtime's fetched main.
expect_failure 'no fetched origin/main' bash "$prepare" --verify --require-published
git init -q --bare "$scratch/origin.git"
git -C "$scratch/source" remote add origin "$scratch/origin.git"
git -C "$scratch/source" push -q origin "$revision:refs/heads/main"
git -C "$scratch/source" fetch -q origin
expect_failure 'not reachable from the runtime' bash "$prepare" --verify --require-published
git -C "$scratch/source" push -q origin "$candidate:refs/heads/main"
git -C "$scratch/source" fetch -q origin
bash "$prepare" --verify --require-published
expect_failure 'applies only to the locked' bash "$prepare" --verify --development --require-published

# The wrapper must resolve the selected installed Cargo, even when a different
# Cargo/rustc is ahead of rustup proxies in the caller's PATH. +stable is not
# accepted by actual Cargo binaries, only by rustup proxies.
mkdir -p "$scratch/toolchain/bin"
cp "$scratch/bin/cargo" "$scratch/toolchain/bin/cargo"
sed -e 's/echo fixture/echo "rustc selected-toolchain"/' \
  -e 's/FIXTURE_RUST_HOST-aarch64-apple-darwin/FIXTURE_RUST_HOST-x86_64-apple-darwin/' \
  "$scratch/bin/rustc" > "$scratch/toolchain/bin/rustc"
chmod +x "$scratch/toolchain/bin/rustc"
cat > "$scratch/bin/rustup" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == which && "$2" == --toolchain && "$3" == stable ]]
printf '%s/%s\n' "$FIXTURE_TOOLCHAIN" "$4"
STUB
chmod +x "$scratch/bin/rustup"
env RUSTUP_TOOLCHAIN=stable FIXTURE_TOOLCHAIN="$scratch/toolchain/bin" \
  FIXTURE_EXPECTED_TARGET=aarch64-apple-darwin \
  EXPECTED_CARGO="$scratch/toolchain/bin/cargo" EXPECTED_RUSTC="$scratch/toolchain/bin/rustc" \
  EXPECTED_WRAPPER="${RUSTC_WRAPPER:-}" bash "$prepare"
jq -e '.toolchain.rustc == "rustc selected-toolchain"' "$scratch/output/runtime-provenance.json" >/dev/null
env RUSTUP_TOOLCHAIN=stable FIXTURE_TOOLCHAIN="$scratch/toolchain/bin" \
  MEMEX_RUNTIME_NATIVE_HOST_QUALIFICATION=1 ARCHES=x86_64 FIXTURE_EXPECTED_TARGET= \
  CARGO_TARGET_DIR="$scratch/selected-native" \
  EXPECTED_CARGO="$scratch/toolchain/bin/cargo" EXPECTED_RUSTC="$scratch/toolchain/bin/rustc" \
  EXPECTED_WRAPPER="${RUSTC_WRAPPER:-}" bash "$prepare"
env RUSTUP_TOOLCHAIN=stable FIXTURE_TOOLCHAIN="$scratch/toolchain/bin" \
  FIXTURE_EXPECTED_TARGET=aarch64-apple-darwin \
  EXPECTED_CARGO="$scratch/toolchain/bin/cargo" EXPECTED_RUSTC="$scratch/toolchain/bin/rustc" \
  RUSTC_WRAPPER=fixture-wrapper EXPECTED_WRAPPER=fixture-wrapper bash "$prepare"
jq -e '.toolchain.cargoDriver == "cargo" and .toolchain.rustc == "rustc selected-toolchain"' \
  "$scratch/output/runtime-provenance.json" >/dev/null
# Verification warnings can accompany exit0. Neither divergence nor a missing
# report may publish a new provenance file or replace the prepared archive.
verification_output="$scratch/verification-output"
env MBX_VERIFY=1 MEMEX_RUNTIME_OUTPUT="$verification_output" \
  MBX_STATS_REPORT="$scratch/requested-report.json" bash "$prepare"
jq -e '.verifications == 6 and .divergences == 0' "$scratch/requested-report.json" >/dev/null
cp "$verification_output/runtime-provenance.json" "$scratch/prior-provenance"
cp "$verification_output/libsq_acp_runtime.a" "$scratch/prior-library"
expect_failure 'Boxington verification failed' env MBX_VERIFY=1 FIXTURE_DIVERGENCES=6 \
  MEMEX_RUNTIME_OUTPUT="$verification_output" bash "$prepare"
cmp "$scratch/prior-provenance" "$verification_output/runtime-provenance.json"
cmp "$scratch/prior-library" "$verification_output/libsq_acp_runtime.a"
expect_failure 'Boxington verification failed' env MBX_VERIFY=1 FIXTURE_MISSING_REPORT=1 \
  MEMEX_RUNTIME_OUTPUT="$scratch/missing-report-output" bash "$prepare"
[[ ! -e "$scratch/missing-report-output/runtime-provenance.json" ]]
expect_failure 'Boxington verification failed' env MBX_VERIFY=1 FIXTURE_DIVERGENCES=invalid \
  MEMEX_RUNTIME_OUTPUT="$scratch/invalid-report-output" bash "$prepare"
[[ ! -e "$scratch/invalid-report-output/runtime-provenance.json" ]]
expect_failure 'Boxington verification failed' env MBX_VERIFY=1 ARCHES='arm64 x86_64' \
  FIXTURE_DIVERGENT_TARGET=x86_64-apple-darwin MEMEX_RUNTIME_OUTPUT="$scratch/multi-output" bash "$prepare"
[[ ! -e "$scratch/multi-output/runtime-provenance.json" ]]
# An explicitly selected non-mbx wrapper must not be held to mbx's report API.
env MBX_VERIFY=1 RUSTC_WRAPPER=fixture-wrapper FIXTURE_MISSING_REPORT=1 \
  MEMEX_RUNTIME_OUTPUT="$scratch/other-wrapper-output" bash "$prepare"
echo "Runtime provenance contract tests passed"
