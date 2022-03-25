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
RUN apt-get update && apt-get -qy install ca-certificates curl sqlite3 tini

COPY kdestroy kinit materialized /usr/local/bin/

ENTRYPOINT ["tini", "--", "materialized", "--log-file=stderr", "--listen-addr=0.0.0.0:6875"]
