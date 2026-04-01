# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Lean 4 proof checker for formal verification of type system properties.

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y curl git && rm -rf /var/lib/apt/lists/* && \
    curl -sSf https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh | \
    bash -s -- -y --default-toolchain leanprover/lean4:v4.16.0

ENV PATH="/root/.elan/bin:${PATH}"

WORKDIR /spec

ENTRYPOINT ["lake"]
