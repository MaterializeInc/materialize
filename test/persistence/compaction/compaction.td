# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Smoke tests that verify compaction occurs as expected.
#
# Right now this test only verifies that user tables are hooked up together properly
# and will compact stored data. There's more detailed tests that check exactly
# how the persist codebase does compaction in src/persist.

$ set-sql-timeout duration=30s

> CREATE MATERIALIZED VIEW most_recent_mz_metrics AS
  SELECT * FROM
  (SELECT DISTINCT metric FROM mz_metrics) grp,
  LATERAL (
    SELECT value FROM mz_metrics
    WHERE metric = grp.metric
    ORDER BY time DESC LIMIT 1)

> CREATE TABLE compaction (f1 TEXT);

# At the beginning there's fewer than 200 bytes in the stored arrangement.
> SELECT value < 200 AS result FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_bytes'
result
-----
true

# Insert over 1 MiB of data.
> INSERT INTO compaction VALUES (repeat('this is a compaction test. ', 50000));

# There's at least 1 MiB in the stored arrangement.
> SELECT value > 1 << 20 AS result FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_bytes'
result
-----
true

# Delete all the values from the table.
> DELETE FROM compaction;

# After deletion and compaction there's fewer than 200 bytes in the stored arrangement.
> SELECT value < 200 AS result FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_bytes'
result
-----
true
