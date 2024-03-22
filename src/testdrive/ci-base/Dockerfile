# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a separate mzimage so that we don't have to re-install the apt things
# every time we get a CI builder with a cold cache.

MZFROM ubuntu-base

RUN apt-get update && apt-get -qy install --no-install-recommends \
    ca-certificates \
    curl \
    dnsutils \
    iputils-ping \
    llvm \
    postgresql-client \
    ssh \
    wait-for-it \
    wget
