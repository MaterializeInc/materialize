# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build and run SQLsmith ported for Materialize

MZFROM ubuntu-base

WORKDIR /workdir

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    cmake \
    make \
    g++ \
    libpqxx-dev \
    nlohmann-json3-dev

# Rebuild since SQLsmith repo might have changed
ADD https://api.github.com/repos/def-/sqlsmith/git/refs/heads/materialize version.json

# Build SQLsmith
RUN git clone --depth=1 --single-branch --branch=materialize https://github.com/def-/sqlsmith \
    && cd sqlsmith \
    && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_COMPILER=c++ . \
    && cmake --build . -j `nproc`

ENTRYPOINT ["sqlsmith/sqlsmith"]
