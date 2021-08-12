# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Boilerplate

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"a", "type":"string"}
        ]
    }

$ kafka-create-topic topic=input

# Verify that builtin tables, views, and sources are always available.

> SELECT COUNT(*) from mz_sources WHERE status = 'created' and id LIKE 's%'
0

> SELECT COUNT(*) > 0 from mz_sources WHERE status = 'available' and id LIKE 's%'
true

> SELECT COUNT(*) from mz_views WHERE status = 'created' and id LIKE 's%'
0

> SELECT COUNT(*) > 0 from mz_views WHERE status = 'available' and id LIKE 's%'
true

> SELECT COUNT(*) from mz_tables WHERE status = 'created' and id LIKE 's%'
0

> SELECT COUNT(*) > 0 from mz_tables WHERE status = 'available' and id LIKE 's%'
true

# Non-materialized sources are not available

> CREATE SOURCE non_materialized_source (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> SELECT name, status FROM mz_sources WHERE name = 'non_materialized_source'
name status
------------
non_materialized_source created

# creating an index makes sources available

> CREATE DEFAULT INDEX ON non_materialized_source

> SELECT name, status FROM mz_sources WHERE name = 'non_materialized_source'
name status
------------
non_materialized_source available

> CREATE INDEX my_idx ON non_materialized_source (a)

> DROP INDEX non_materialized_source_primary_idx

# dropping an index while others are still available makes the source stay
# available
> SELECT name, status FROM mz_sources WHERE name = 'non_materialized_source'
name status
------------
non_materialized_source available

# Materialized sources are available right away

> CREATE MATERIALIZED SOURCE materialized_source (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> SELECT name, status FROM mz_sources WHERE name = 'materialized_source'
name status
------------
materialized_source available

# dropping the index makes sources unavailable

> DROP INDEX materialized_source_primary_idx

> SELECT name, status FROM mz_sources WHERE name = 'materialized_source'
name status
------------
materialized_source created

# Tables are always available

> CREATE TABLE test_table (a text)

> SELECT name, status FROM mz_tables WHERE name = 'test_table'
name status
------------
test_table available

# Non-materialized views are not available

> CREATE SOURCE view_source (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> CREATE VIEW non_materialized_view as
  SELECT * FROM view_source

> SELECT name, status FROM mz_views WHERE name = 'non_materialized_view'
name status
------------
non_materialized_view created

# creating an index makes views available

> CREATE DEFAULT INDEX ON non_materialized_view

> SELECT name, status FROM mz_views WHERE name = 'non_materialized_view'
name status
------------
non_materialized_view available

# Materialized views are available right away

> CREATE MATERIALIZED VIEW materialized_view as
  SELECT * FROM view_source

> SELECT name, status FROM mz_views WHERE name = 'materialized_view'
name status
------------
materialized_view available

# dropping the index makes views unavailable

> DROP INDEX materialized_view_primary_idx

> SELECT name, status FROM mz_views WHERE name = 'materialized_view'
name status
------------
materialized_view created

# Views that only have materialized inputs are available

> CREATE MATERIALIZED SOURCE materialized_source_for_view (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> CREATE VIEW non_materialized_view_from_materialized AS
  SELECT * FROM materialized_source_for_view

> SELECT name, status FROM mz_views WHERE name = 'non_materialized_view_from_materialized'
name status
------------
non_materialized_view_from_materialized available


# Complex view with multiple transitive inputs

> CREATE SOURCE complex_non_materialized_source1 (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> CREATE SOURCE complex_non_materialized_source2 (a)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

> CREATE MATERIALIZED VIEW complex_intermediate_view1 AS
  SELECT * FROM complex_non_materialized_source1

> CREATE VIEW complex_intermediate_view2 AS
  SELECT * FROM complex_non_materialized_source2

> CREATE VIEW complex_combined_view AS
  SELECT * FROM complex_intermediate_view1
  UNION ALL
  SELECT * FROM complex_intermediate_view2

# one of the dependent paths is not yet available
> SELECT name, status FROM mz_views WHERE name = 'complex_combined_view'
name status
------------
complex_combined_view created

> CREATE DEFAULT INDEX ON complex_intermediate_view2

# now all dependents is available
> SELECT name, status FROM mz_views WHERE name = 'complex_combined_view'
name status
------------
complex_combined_view available
