# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# https://github.com/MaterializeInc/mzcli
FROM materialize/mzcli

RUN apt-get update -o Acquire::Languages=none \
    && apt-get install -qy --no-install-recommends \
        less postgresql-client \
    && apt-get autoremove -qy \
    && rm -rf \
        /var/cache/apt/archives \
        /var/lib/apt/lists/* \
    ;

COPY watch-sql /usr/local/bin/watch-sql
