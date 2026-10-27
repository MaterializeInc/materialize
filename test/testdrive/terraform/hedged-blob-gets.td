# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Hedged blob gets against the deployment's real blob store: every replica
# process that reads from it has a working second client, forced hedges fire,
# and nothing failed.
#
# Only meant for the terraform tests (test/terraform/mzcompose.py). Against a
# local blob store a hedge rarely wins, so the testdrive suite must not run
# this file; its `*.td` glob does not descend into this directory.
#
# The terraform tests run it after their other testdrive files, so the error
# checks also cover those files' reads.
#
# The hedge metrics come from mz_cluster_prometheus_metrics, i.e. from the
# replicas of the clusters the materialize user can query. environmentd and
# the mz_system cluster are not covered.

# Hedging is on by default from v26.44.
$ skip-if
SELECT mz_version_num() < 2604400

$ postgres-execute connection=mz_system
ALTER SYSTEM SET compute_prometheus_introspection_scrape_interval = '1s'

> CREATE TABLE hedge_check (a int)

> INSERT INTO hedge_check SELECT generate_series(1, 100000)

# A replica arms its second client when it first opens the blob store, and the
# keep-warm cycle sets the round-trip gauge.
> SET cluster = quickstart

> SELECT sum(a) FROM hedge_check
5000050000

> SELECT bool_and(value = 1) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedge_armed'
true

> SELECT bool_and(value > 0) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedge_rtt_latency'
true

> SET cluster = mz_catalog_server

> SELECT bool_and(value = 1) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedge_armed'
true

> SELECT bool_and(value > 0) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedge_rtt_latency'
true

# Hedge every get. With the blob cache off, each retry of the query below
# fetches the table's parts again and so fires new hedges.
# NOTE: `count(*)` would not do: it reads no parts.
# NOTE: this checks that hedges fire, not that one wins. Whether a hedge beats
# its primary depends on the store's latency spread: against Azure Blob, the
# primaries of these sequential reads were observed to always finish first.
# The fire counter is cumulative and earlier test files can fire hedges at the
# default delay, hence the comparison against a count taken before forcing.
> SET cluster = quickstart

$ set-from-sql var=hedges-fired-before
SELECT coalesce(sum(value), 0)::text FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedges_fired'

$ postgres-execute connection=mz_system
ALTER SYSTEM SET persist_blob_hedged_get_delay = '0s'
ALTER SYSTEM SET persist_blob_hedged_get_budget_ratio = 1
ALTER SYSTEM SET persist_blob_cache_mem_limit_bytes = 0

> SELECT (SELECT sum(a) FROM hedge_check) = 5000050000 AND (SELECT sum(value) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name = 'mz_persist_blob_hedges_fired') > ${hedges-fired-before}
true

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET persist_blob_hedged_get_delay
ALTER SYSTEM RESET persist_blob_hedged_get_budget_ratio
ALTER SYSTEM RESET persist_blob_cache_mem_limit_bytes

# No hedge or keep-warm get failed, and no process failed to open its second
# client (which would skip hedges as unavailable).
> SET cluster = quickstart

> SELECT sum(value) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name IN ('mz_persist_blob_hedge_errors', 'mz_persist_blob_hedge_warm_errors') OR (metric_name = 'mz_persist_blob_hedges_skipped' AND labels->'reason' = 'unavailable')
0

> SET cluster = mz_catalog_server

> SELECT sum(value) FROM mz_introspection.mz_cluster_prometheus_metrics WHERE metric_name IN ('mz_persist_blob_hedge_errors', 'mz_persist_blob_hedge_warm_errors') OR (metric_name = 'mz_persist_blob_hedges_skipped' AND labels->'reason' = 'unavailable')
0

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET compute_prometheus_introspection_scrape_interval
