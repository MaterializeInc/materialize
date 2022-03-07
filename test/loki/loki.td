# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ loki-ingest job=foo node=node1
first line
second line

$ loki-ingest job=bar node=node1
first line

$ loki-ingest job=foo node=node2
first line

$ loki-ingest job=bar node=node2
first line
second line

# The raw source that brings in data as a single text column containing JSON
> CREATE SOURCE loki_system_logs_raw
  FROM LOKI
  ADDRESS '${testdrive.loki-addr}'
  QUERY '{job="foo"}'
  FORMAT TEXT;

# Extract the JSON fields
> CREATE VIEW loki_system_logs AS
  SELECT
    val->>'text' AS text,
    val->'labels' AS labels
  FROM (SELECT text::jsonb AS val FROM loki_system_logs_raw);

# Create a materialized aggregation
> CREATE MATERIALIZED VIEW logs_by_filename AS
  SELECT labels->>'job' AS job,
    labels->>'node' AS node,
    COUNT(*) AS n FROM loki_system_logs
  GROUP BY labels->>'job', labels->>'node';

> SELECT n FROM logs_by_filename WHERE job = 'foo' AND node = 'node1';
2

$ loki-ingest job=foo node=node1
another line

# See the data change
> SELECT n FROM logs_by_filename WHERE job = 'foo' AND node = 'node1';
3

# Test that the volatility bit is set correctly.
# > SELECT name, volatility FROM mz_sources WHERE name = 'market_orders_raw'
# market_orders_raw   volatile
# > SELECT name, volatility from mz_views WHERE name = 'avg_bid'
# avg_bid   volatile
# > CREATE MATERIALIZED VIEW nonvol AS SELECT 1
# > SELECT name, volatility from mz_views WHERE name = 'nonvol'
# nonvol   nonvolatile
# > CREATE MATERIALIZED VIEW depends_vol_nonvol AS SELECT * FROM nonvol, avg_bid
# > SELECT name, volatility from mz_views WHERE name = 'depends_vol_nonvol'
# depends_vol_nonvol   volatile
# $ file-append path=test
# ignored
# > CREATE MATERIALIZED SOURCE file_src FROM FILE '${testdrive.temp-dir}/test' FORMAT BYTES
# > SELECT name, volatility FROM mz_sources WHERE name = 'file_src'
# file_src  unknown
# > CREATE MATERIALIZED VIEW depends_nonvol_unknown AS SELECT * FROM nonvol, file_src
# > SELECT name, volatility from mz_views WHERE name = 'depends_nonvol_unknown'
# depends_nonvol_unknown   unknown
# > CREATE MATERIALIZED VIEW depends_vol_unknown AS SELECT * FROM avg_bid, file_src
# > SELECT name, volatility from mz_views WHERE name = 'depends_vol_unknown'
# depends_vol_unknown   volatile


