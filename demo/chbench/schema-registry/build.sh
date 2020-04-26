#!/usr/bin/env bash

set -euo pipefail

url="https://github.com/stedolan/jq/releases/download/jq-${JQ_VERSION}/jq-linux64"
out=dist/jq

if command -v curl >/dev/null; then
    curl -fL "$url" > "$out"
elif command -v wget >/dev/null; then
    wget -qO- "$url" > "$out"
else
    echo "Unable to download jq"
    exit 1
fi

chmod 755 "$out"
