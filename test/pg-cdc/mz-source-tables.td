# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that the mz_source_*  tables are populated for Postgres sources
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';


> SELECt id LIKE 'u%', oid > 0, schema_id > 1, volatility = 'unknown' FROM mz_sources WHERE name = 'mz_source';
true true true true

> SELECT partition_id IS NULL, "offset" = timestamp, timestamp > 0, source_id like 'u%', dataflow_id > 0 FROM mz_source_info WHERE source_name LIKE '%postgres%';
true true true true true
