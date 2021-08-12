#!/usr/bin/env bash

# Updates to the hardcoded version of libbsd.

set -euo pipefail

cd "$(dirname "$0")"

version=0.11.3

set -x
for ext in tar.xz tar.xz.asc; do
    curl -fsSL "https://libbsd.freedesktop.org/releases/libbsd-$version.$ext" > "libbsd.$ext"
done

gpg --verify libbsd.tar.xz.asc libbsd.tar.xz

tar tf libbsd.tar.xz
tar --strip-components=1 -C libbsd -xf libbsd.tar.xz libbsd-"$version"/src/{flopen,pidfile,progname}.c
rm libbsd.tar.xz libbsd.tar.xz.asc
