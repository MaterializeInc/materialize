# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
