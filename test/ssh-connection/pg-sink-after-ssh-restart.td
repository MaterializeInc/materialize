# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> ALTER CLUSTER pgsink_cluster SET (REPLICATION FACTOR 0)
> ALTER CLUSTER pgsink_cluster SET (REPLICATION FACTOR 1)

> SELECT * FROM pgsink_keyed_readback
1 recovered
4 four
> SELECT * FROM pgsink_keyless_readback
four
recovered

> SELECT name, status, error FROM mz_internal.mz_sink_statuses WHERE name LIKE 'pgsink_%'
pgsink_keyed running <null>
pgsink_keyless running <null>
