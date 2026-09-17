#!/usr/bin/env bash
# Download and extract linuxdeploy, its Qt plugin, and appimagetool.
# AppImages are extracted (no FUSE needed) so the tools also run on
# systems without libfuse2, such as CI runners.
#
# Layout: each tool extracts to <tools-dir>/<name>.AppDir, with executable
# entry points symlinked from <tools-dir>/bin. The two levels stay separate:
# pointing a symlink at an existing directory would nest it instead of
# replacing it, so entry points must never share a path with extraction dirs.
#
# Usage: install-linuxdeploy.sh [tools-dir]
# Env overrides: LINUXDEPLOY_VERSION, LINUXDEPLOY_PLUGIN_QT_VERSION,
#   APPIMAGETOOL_VERSION (default: continuous, i.e. a moving target, not a
#   pin). For reproducible builds, set an explicit version plus its digest:
#   LINUXDEPLOY_SHA256, LINUXDEPLOY_PLUGIN_QT_SHA256, APPIMAGETOOL_SHA256.
set -euo pipefail
app_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tools_dir="${1:-$app_dir/build/tools}"
case "$(uname -s)" in
    Linux) ;;
    *) echo 'linuxdeploy packaging requires a Linux host.' >&2; exit 2 ;;
esac
case "$(uname -m)" in
    x86_64) deploy_arch=x86_64 ;;
    aarch64) deploy_arch=aarch64 ;;
    *) echo "Unsupported architecture for linuxdeploy: $(uname -m)" >&2; exit 2 ;;
esac
linuxdeploy_version="${LINUXDEPLOY_VERSION:-continuous}"
plugin_qt_version="${LINUXDEPLOY_PLUGIN_QT_VERSION:-continuous}"
appimagetool_version="${APPIMAGETOOL_VERSION:-continuous}"

fetch() {
    local url="$1" dest="$2" digest="${3:-}"
    if [[ -f "$dest" ]]; then
        echo "Reusing $dest"
    else
        echo "Downloading $url"
        curl -fsSL --retry 3 -o "$dest" "$url"
        chmod +x "$dest"
    fi
    if [[ -n "$digest" ]]; then
        echo "$digest  $dest" | sha256sum -c -
    fi
}

extract_tool() {
    local image="$1" name="$2"
    local target="$tools_dir/$name.AppDir"
    if [[ -x "$target/AppRun" ]]; then
        echo "Reusing $target"
        return
    fi
    local work
    work="$(mktemp -d "$tools_dir/extract.XXXXXX")"
    trap 'rm -rf "$work"' RETURN
    (cd "$work" && "$image" --appimage-extract >/dev/null)
    rm -rf "$target"
    mv "$work/squashfs-root" "$target"
}

mkdir -p "$tools_dir" "$tools_dir/bin"
base_tmp="$(mktemp -d "$tools_dir/download.XXXXXX")"
trap 'rm -rf "$base_tmp"' EXIT
deploy_image="$base_tmp/linuxdeploy.AppImage"
plugin_image="$base_tmp/plugin-qt.AppImage"
tool_image="$base_tmp/appimagetool.AppImage"
fetch "https://github.com/linuxdeploy/linuxdeploy/releases/download/${linuxdeploy_version}/linuxdeploy-${deploy_arch}.AppImage" "$deploy_image" "${LINUXDEPLOY_SHA256:-}"
fetch "https://github.com/linuxdeploy/linuxdeploy-plugin-qt/releases/download/${plugin_qt_version}/linuxdeploy-plugin-qt-${deploy_arch}.AppImage" "$plugin_image" "${LINUXDEPLOY_PLUGIN_QT_SHA256:-}"
fetch "https://github.com/AppImage/appimagetool/releases/download/${appimagetool_version}/appimagetool-${deploy_arch}.AppImage" "$tool_image" "${APPIMAGETOOL_SHA256:-}"
extract_tool "$deploy_image" "linuxdeploy"
extract_tool "$plugin_image" "linuxdeploy-plugin-qt"
extract_tool "$tool_image" "appimagetool"
# The Qt plugin ships patchelf 0.15.0 independently of linuxdeploy. On
# Fedora x86_64 it relocates .init without updating DT_INIT, so packaged
# Qt plugins segfault when loaded. Use linuxdeploy's newer copy for both.
deploy_patchelf="$tools_dir/linuxdeploy.AppDir/usr/bin/patchelf"
patchelf_version="$("$deploy_patchelf" --version | awk '{print $2}')"
if [[ -z "$patchelf_version" ]] || ! printf '%s\n' 0.19 "$patchelf_version" | sort -VC; then
    echo "linuxdeploy must bundle patchelf >= 0.19 (found $patchelf_version)." >&2
    exit 1
fi
ln -sfn ../../../linuxdeploy.AppDir/usr/bin/patchelf \
    "$tools_dir/linuxdeploy-plugin-qt.AppDir/usr/bin/patchelf"
echo "Using linuxdeploy's patchelf $patchelf_version for Qt deployment"
# The tools bundle an ancient binutils strip that fails on modern system
# libraries (`.relr.dyn`: "unknown type [0x13]"). Point the extracted
# tools at the host strip, which matches the host toolchain that built
# those libraries. Keeps stripping (unlike NO_STRIP) with a working tool.
if host_strip="$(command -v strip)"; then
    for appdir in "$tools_dir"/*.AppDir; do
        if [[ -f "$appdir/usr/bin/strip" && ! -L "$appdir/usr/bin/strip" ]]; then
            echo "Redirecting bundled strip to host strip: $appdir/usr/bin/strip"
            ln -sfn "$host_strip" "$appdir/usr/bin/strip"
        fi
    done
fi
bin_dir="$tools_dir/bin"
ln -sfn "../linuxdeploy.AppDir/AppRun" "$bin_dir/linuxdeploy"
ln -sfn "../linuxdeploy-plugin-qt.AppDir/AppRun" "$bin_dir/linuxdeploy-plugin-qt"
ln -sfn "../appimagetool.AppDir/AppRun" "$bin_dir/appimagetool"
for tool in linuxdeploy linuxdeploy-plugin-qt appimagetool; do
    if [[ ! -x "$bin_dir/$tool" || -d "$bin_dir/$tool" ]]; then
        echo "Tool entry point is not an executable file: $bin_dir/$tool" >&2
        exit 1
    fi
done
printf 'Tools ready in %s (entry points in %s)\n' "$tools_dir" "$bin_dir"
