#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# slt.sh — runs sqllogictest in CI.

set -euo pipefail

verbosity=-vv
if [[ "${BUILDKITE-}" ]]; then
    verbosity=-v

    # TODO(benesch): share with the deploy pipeline.
    mkdir ~/.ssh
    cat >> ~/.ssh/known_hosts <<EOF
    mtrlz.dev ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCsJdMnBVoCu1StAcoSH9iMIdM94V3if6U87gdrucDV/NHeWSTZa3lxHV4mGAFfSBV0oF3y82Sx4gspTjBxAeqwLcm70DrtSzl4cEPiJo4Hgj9U54kuZcIpDP8He/rNYzncrzKoeKzOwtVH5+oaJPcfeZ2CI1BVGj/Xt2A44d8Bixtzc6r+Q9czzYIwTCSxN4+rr7wPnNT3QXF+EvmmIsChXfsjVJpFHQKygVd9+9hk+b/bnqjsMG0O2gHmXSh4FQgbjeJvdTLEqC6SN8VEkaLLosbn79gxjei+U8pHcnossNgJQiaj4N5MDQwzWwGV9dxFBvvxcpdUknIBFlML5Joj
    mtrlz.dev ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBMl4Vm8V+nUA677qNBKcIjUACY1tZuUa74MVMScGUp+szeuPbkqHpYqSkGnU+ONYVT0nRmXFhIOU2cO6hi2lGs4=
    mtrlz.dev ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPbUN6RkJgvRv3Y7pmnwth5nribjue21mf8BeD12qr2C
EOF
fi

set -x

mkdir -p target

# TODO(jamii) We can't run test/select4.test without predicate pushdown - it's too slow
cargo run --release --bin=sqllogictest -- \
    sqllogictest/test/select1.test \
    sqllogictest/test/select2.test \
    sqllogictest/test/select3.test \
    sqllogictest/test/select5.test \
    sqllogictest/test/evidence \
    sqllogictest/test/index \
    sqllogictest/test/random \
    test/*.slt \
    "$verbosity" "$@" | tee target/slt.out || true

if [[ "${BUILDKITE_BRANCH-}" = master && "${BUILDKITE_COMMIT-}" ]]; then
    template="INSERT INTO slt (commit, unsupported, parse_failure, plan_failure, inference_failure, output_failure, bail, success) VALUES ('$BUILDKITE_COMMIT', \\1, \\2, \\3, \\4, \\5, \\6, \\7);"
    sql=$(tail -n1 target/slt.out \
        | sed -E "s/^.*unsupported=([0-9]+) parse-failure=([0-9]+) plan-failure=([0-9]+) inference-failure=([0-9]+) output-failure=([0-9]+) bail=([0-9]+) success=([0-9]+).*$/$template/")
    ssh buildkite@mtrlz.dev psql <<< "$sql"
fi
