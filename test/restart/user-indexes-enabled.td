# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# 🔬 Views

> SELECT count(*) FROM mz_databases
1

> CREATE VIEW IF NOT EXISTS logging_derived AS SELECT count(*) FROM mz_databases
> SELECT * FROM logging_derived
1

> CREATE MATERIALIZED VIEW IF NOT EXISTS logging_derived_mat AS SELECT count(*) FROM mz_databases
> SELECT * FROM logging_derived_mat
1

> CREATE VIEW constant AS SELECT 1
> SELECT * FROM constant
1

# 🔬 Views + Sources

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 1}

> CREATE SOURCE IF NOT EXISTS data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW IF NOT EXISTS mat_view AS
  SELECT sum(a) FROM data

> SELECT * FROM mat_view
1

# Enabling indexes is idempotent
> ALTER INDEX mat_view_primary_idx SET ENABLED;

> SELECT * FROM mat_view
1

> SHOW MATERIALIZED VIEWS
mat_view
logging_derived_mat

> CREATE MATERIALIZED SOURCE IF NOT EXISTS mat_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> SELECT * FROM mat_data
1

> SHOW MATERIALIZED SOURCES
mat_data

$ set join-schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  }

$ kafka-create-topic topic=join-data

$ kafka-ingest format=avro topic=join-data schema=${join-schema} timestamp=1
{"a": 1, "b": "a"}

> CREATE MATERIALIZED SOURCE IF NOT EXISTS join_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-join-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${join-schema}'

> CREATE VIEW join_view AS
    SELECT join_data.a, join_data.b
    FROM join_data
    JOIN mat_data
    ON mat_data.a = join_data.a;

> SELECT * FROM join_view;
1 a

# 🔬 Sinks

> CREATE SINK snk_indexes_enabled FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk_indexes_enabled'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.snk_indexes_enabled sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}

# 🔬 Tables

> CREATE TABLE IF NOT EXISTS t (a int)

> INSERT INTO t VALUES (1)

> SELECT * FROM t
1

# 🔬 Indexes to drop
> CREATE INDEX mv_drop_idx ON mat_view(sum);
> CREATE INDEX ms_drop_idx ON mat_data(a);
> CREATE INDEX t_drop_idx ON t(a);
