# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic connector functionality

###
# Test core functionality by creating, introspecting and dropping a connector
###
$ kafka-create-topic topic=connector_test partitions=1
$ kafka-ingest format=bytes topic=connector_test
1,2
2,3

> CREATE CONNECTOR testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> SELECT name, connector_type from mz_connectors
name  connector_type
------------------------------
testconn   kafka

> SHOW CREATE CONNECTOR testconn
Connector   "Create Connector"
---------------------------------
materialize.public.testconn   "CREATE CONNECTOR \"materialize\".\"public\".\"testconn\" FOR KAFKA BROKER '${testdrive.kafka-addr}'"


> DROP CONNECTOR testconn

###
# Test that connectors work in creating a source
###
> CREATE CONNECTOR testconn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE MATERIALIZED SOURCE connector_source (first, second)
  FROM KAFKA CONNECTOR testconn
  TOPIC 'testdrive-connector_test-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM connector_source
first second mz_offset
----------------------
1     2      1
2     3      2

# Confirm we cannot drop the connector while a source depends upon it
! DROP CONNECTOR testconn;
contains:depended upon by catalog item 'materialize.public.connector_source'

# Confirm the drop works if we add cascade
> DROP CONNECTOR testconn CASCADE;

# Validate the cascading drop actually happened
! SELECT * FROM connector_source
contains:unknown catalog item 'connector_source'
