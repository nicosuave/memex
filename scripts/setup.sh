#!/bin/sh
set -e

REPO="nicosuave/memex"
BINARY="memex"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Darwin) OS="macos" ;;
    *) echo "Unsupported OS: $OS (only macOS is supported)"; exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    arm64|aarch64) ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Get latest version
VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
    echo "Failed to get latest version"
    exit 1
fi

echo "Installing $BINARY v$VERSION for $OS-$ARCH..."

# Download and extract
URL="https://github.com/$REPO/releases/download/v$VERSION/$BINARY-$VERSION-$OS-$ARCH.tar.gz"
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

curl -fsSL "$URL" | tar -xz -C "$TMP_DIR"

mkdir -p "$INSTALL_DIR"
mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/$BINARY"

echo "Installed $BINARY to $INSTALL_DIR/$BINARY"

# Check if ~/.local/bin is in PATH
case ":$PATH:" in
    *":$HOME/.local/bin:"*) ;;
    *)
        echo ""
        echo "Note: $INSTALL_DIR is not in your PATH."
        echo "Add this to your shell config (~/.bashrc, ~/.zshrc, etc.):"
        echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
        ;;
esac

echo ""
echo "Run 'memex skill-install' to install the Claude Code skill"
