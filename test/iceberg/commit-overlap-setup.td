# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE overlap_src (id INT, val TEXT);

> INSERT INTO overlap_src VALUES (1, 'a'), (2, 'b'), (3, 'c');

# APPEND exposes duplicate publication instead of masking it with keyed upserts.
> CREATE SINK overlap_sink
    FROM overlap_src
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'overlap_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '2s');

# The reconstruction proof must run with replica-owned execution, not a
# controller replaying the sink after observing its health status.
> SELECT EXISTS (
    SELECT 1 FROM mz_internal.mz_catalog_raw
    WHERE data->>'kind' = 'ClientIncarnation'
      AND data->'value'->>'replica_id' IS NOT NULL
  );
true
