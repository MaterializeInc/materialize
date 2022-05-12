# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

# We install sqlite3 to allow debugging of the catalog file, if necessary.
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install \
        ca-certificates \
        curl \
        postgresql-14 \
        sqlite3 \
        tini \
    && useradd --system --create-home materialize \
    && pg_dropcluster 14 main \
    && pg_createcluster 14 materialize --user=materialize \
    && pg_ctlcluster 14 materialize start \
    && su materialize -c "createdb materialize" \
    && mkdir /mzdata \
    && chown materialize /mzdata

COPY storaged computed materialized entrypoint.sh /usr/local/bin/

USER materialize

ENTRYPOINT ["tini", "--", "entrypoint.sh"]
