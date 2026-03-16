#!/usr/bin/env bash

set -euo pipefail

REPO="sjwiesman/mz-deploy"
TAP_REPO="sjwiesman/homebrew-mz-deploy"

TARGETS=(
    aarch64-apple-darwin
    x86_64-apple-darwin
    x86_64-unknown-linux-gnu
    aarch64-unknown-linux-gnu
)

linker_pkg_for() {
    case "$1" in
        x86_64-unknown-linux-gnu)  echo "gcc g++" ;;
        aarch64-unknown-linux-gnu) echo "gcc-aarch64-linux-gnu g++-aarch64-linux-gnu" ;;
    esac
}

cargo_linker_for() {
    case "$1" in
        x86_64-unknown-linux-gnu)  echo "x86_64-linux-gnu-gcc" ;;
        aarch64-unknown-linux-gnu) echo "aarch64-linux-gnu-gcc" ;;
    esac
}

# --- Prerequisites ---

for cmd in cargo rustup gh docker jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is not installed." >&2
        exit 1
    fi
done

if ! gh auth status &>/dev/null; then
    echo "Error: gh is not authenticated. Run 'gh auth login'." >&2
    exit 1
fi

if ! docker info &>/dev/null; then
    echo "Error: Docker is not running." >&2
    exit 1
fi

# Ensure macOS targets are installed
for target in aarch64-apple-darwin x86_64-apple-darwin; do
    rustup target add "$target"
done

# --- Version ---

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

VERSION=$(cargo metadata --format-version 1 --no-deps --manifest-path "$WORKSPACE_ROOT/Cargo.toml" | jq -r '.packages[] | select(.name == "mz-deploy") | .version')
TAG="v${VERSION}"

echo "Building mz-deploy ${TAG}..."

# --- Build ---

BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT

for target in "${TARGETS[@]}"; do
    echo "Building for ${target}..."
    case "$target" in
        *-apple-darwin)
            cargo build --release --target "$target" -p mz-deploy \
                --features vendored-openssl --no-default-features \
                --manifest-path "$WORKSPACE_ROOT/Cargo.toml"
            ;;
        *-linux-gnu)
            LINKER_PKG=$(linker_pkg_for "$target")
            LINKER=$(cargo_linker_for "$target")
            docker run --rm \
                --platform linux/amd64 \
                -v "$WORKSPACE_ROOT":/workspace \
                -w /workspace \
                -e "CARGO_TARGET_$(echo "$target" | tr '[:lower:]-' '[:upper:]_')_LINKER=$LINKER" \
                rust:1.89 \
                bash -c "
                    apt-get update -qq && apt-get install -y -qq $LINKER_PKG perl make libclang-dev cmake lld >/dev/null 2>&1
                    rustup target add $target
                    cargo build --release --target $target -p mz-deploy \
                        --features vendored-openssl --no-default-features
                "
            ;;
    esac
done

# --- Package ---

# Store SHA sums in files since bash 3.x lacks associative arrays
SHA_DIR=$(mktemp -d)

for target in "${TARGETS[@]}"; do
    TARBALL="mz-deploy-${TAG}-${target}.tar.gz"
    BINARY="$WORKSPACE_ROOT/target/${target}/release/mz-deploy"

    if [ ! -f "$BINARY" ]; then
        echo "Error: binary not found at ${BINARY}" >&2
        exit 1
    fi

    tar -czf "${BUILD_DIR}/${TARBALL}" -C "$(dirname "$BINARY")" mz-deploy
    SHA=$(shasum -a 256 "${BUILD_DIR}/${TARBALL}" | awk '{print $1}')
    echo "$SHA" > "${SHA_DIR}/${target}"
    echo "  ${TARBALL}: ${SHA}"
done

sha_for() {
    cat "${SHA_DIR}/$1"
}

# --- GitHub Release ---

echo "Creating GitHub release ${TAG}..."
gh release create "$TAG" \
    --repo "$REPO" \
    --title "mz-deploy ${TAG}" \
    --notes "Release of mz-deploy ${TAG}" \
    "${BUILD_DIR}"/mz-deploy-*.tar.gz

# --- Homebrew Formula ---

echo "Generating Homebrew formula..."

SHA_AARCH64_DARWIN=$(sha_for aarch64-apple-darwin)
SHA_X86_64_DARWIN=$(sha_for x86_64-apple-darwin)
SHA_AARCH64_LINUX=$(sha_for aarch64-unknown-linux-gnu)
SHA_X86_64_LINUX=$(sha_for x86_64-unknown-linux-gnu)

FORMULA="class MzDeploy < Formula
  desc \"Deployment tool for Materialize\"
  homepage \"https://github.com/${REPO}\"
  version \"${VERSION}\"
  license \"BSL-1.1\"

  on_macos do
    if Hardware::CPU.arm?
      url \"https://github.com/${REPO}/releases/download/${TAG}/mz-deploy-${TAG}-aarch64-apple-darwin.tar.gz\"
      sha256 \"${SHA_AARCH64_DARWIN}\"
    else
      url \"https://github.com/${REPO}/releases/download/${TAG}/mz-deploy-${TAG}-x86_64-apple-darwin.tar.gz\"
      sha256 \"${SHA_X86_64_DARWIN}\"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url \"https://github.com/${REPO}/releases/download/${TAG}/mz-deploy-${TAG}-aarch64-unknown-linux-gnu.tar.gz\"
      sha256 \"${SHA_AARCH64_LINUX}\"
    else
      url \"https://github.com/${REPO}/releases/download/${TAG}/mz-deploy-${TAG}-x86_64-unknown-linux-gnu.tar.gz\"
      sha256 \"${SHA_X86_64_LINUX}\"
    end
  end

  def install
    bin.install \"mz-deploy\"
  end

  test do
    assert_match version.to_s, shell_output(\"#{bin}/mz-deploy --version\")
  end
end"

# --- Push to tap ---

echo "Pushing formula to ${TAP_REPO}..."

TAP_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR" "$SHA_DIR" "$TAP_DIR"' EXIT

gh repo clone "$TAP_REPO" "$TAP_DIR"
mkdir -p "$TAP_DIR/Formula"
echo "$FORMULA" > "$TAP_DIR/Formula/mz-deploy.rb"

cd "$TAP_DIR"
git add Formula/mz-deploy.rb
git commit -m "Update mz-deploy to ${TAG}"
git push

echo "Done! Users can install with:"
echo "  brew tap ${TAP_REPO/homebrew-/}"
echo "  brew install mz-deploy"
