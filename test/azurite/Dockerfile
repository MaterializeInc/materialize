# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM node:20-slim

RUN apt-get update && \
    apt-get install -y eatmydata netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# TODO(def-) 3.35.0 might be losing data, see https://buildkite.com/materialize/nightly/builds/14157#019aad0a-ad38-429d-8ef2-e81eb26e0bf2
RUN npm install -g azurite@3.33.0
