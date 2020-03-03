#!/bin/bash
set -ev
make release
make test
if [ "$TRAVIS_RUST_VERSION" = "nightly" ]; then
    make benchmark
fi
