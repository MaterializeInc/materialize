# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

# State from smoke.td (with 4 rows) must survive the envd restart and be
# reachable via the freshly-rebuilt committer cache.
> SELECT * FROM committer_mv;
4

> INSERT INTO committer_t VALUES (5);

> SELECT * FROM committer_mv;
5
