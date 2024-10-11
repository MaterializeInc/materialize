#!/usr/bin/env bash

set -euo pipefail; shopt -s nullglob

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "Skipping codesign as macOS not detected."
  exit 0
fi

if [[ ! -d "./.run" ]]; then
  echo "Please run this script from the root of the repository."
  exit 1
fi

for binary in ./target/debug/clusterd ./target/debug/environmentd; do
  echo "Signing $binary..."
  codesign -s - -f \
    --entitlements ./.run/macos-entitlements-data.xml \
    $binary
done
