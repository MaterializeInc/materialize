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

RUN groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize

ARG CI_SANITIZER=none

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install \
        ca-certificates \
        curl \
        tini \
        ssh \
    && if [ "$CI_SANITIZER" != "none" ]; then \
        TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends llvm; \
       fi \
    && mkdir /scratch \
    && chown materialize /scratch \
    && mkdir /mzdata \
    && chown materialize /mzdata
