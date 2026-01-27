# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for Avro schema references in Confluent Schema Registry.
# Schema references allow one schema to reference named types defined in another schema.

$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# =============================================================================
# Test 1: Simple schema reference
# A schema references a type defined in another schema
# =============================================================================

# First, publish the base schema that defines the User record type
$ set user-schema={
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "name", "type": "string"}
    ]
  }

$ schema-registry-publish subject=testdrive-user-${testdrive.seed}-value schema-type=avro
\${user-schema}

# Now create a topic for events that reference the User type
$ kafka-create-topic topic=simple-ref

# Publish the Event schema that references User, and ingest data
$ set event-schema={
    "type": "record",
    "name": "Event",
    "namespace": "com.example",
    "fields": [
      {"name": "event_id", "type": "long"},
      {"name": "user", "type": "com.example.User"}
    ]
  }

$ kafka-ingest format=avro topic=simple-ref schema=${event-schema} references=testdrive-user-${testdrive.seed}-value timestamp=1
{"event_id": 1, "user": {"id": 100, "name": "Alice"}}
{"event_id": 2, "user": {"id": 101, "name": "Bob"}}

> BEGIN
> CREATE SOURCE simple_ref_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-ref-${testdrive.seed}')

> CREATE TABLE simple_ref FROM SOURCE simple_ref_src (REFERENCE "testdrive-simple-ref-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT event_id, (user).id, (user).name FROM simple_ref ORDER BY event_id
event_id  id   name
-----------------------
1         100  Alice
2         101  Bob

# =============================================================================
# Test 2: Chained schema references (A -> B -> C)
# Schema C references B, which references A
# =============================================================================

# Schema A: Address
$ set address-schema={
    "type": "record",
    "name": "Address",
    "namespace": "com.example",
    "fields": [
      {"name": "street", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "zip", "type": "string"}
    ]
  }

$ schema-registry-publish subject=testdrive-address-${testdrive.seed}-value schema-type=avro
\${address-schema}

# Schema B: Person (references Address)
$ set person-schema={
    "type": "record",
    "name": "Person",
    "namespace": "com.example",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "home_address", "type": "com.example.Address"}
    ]
  }

$ schema-registry-publish subject=testdrive-person-${testdrive.seed}-value schema-type=avro references=testdrive-address-${testdrive.seed}-value
\${person-schema}

# Schema C: Order (references Person, which transitively references Address)
$ kafka-create-topic topic=chained-ref

$ set order-schema={
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
      {"name": "order_id", "type": "long"},
      {"name": "customer", "type": "com.example.Person"}
    ]
  }

$ kafka-ingest format=avro topic=chained-ref schema=${order-schema} references=testdrive-address-${testdrive.seed}-value,testdrive-person-${testdrive.seed}-value timestamp=1
{"order_id": 1001, "customer": {"name": "Charlie", "home_address": {"street": "123 Main St", "city": "Springfield", "zip": "12345"}}}

> BEGIN
> CREATE SOURCE chained_ref_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-chained-ref-${testdrive.seed}')

> CREATE TABLE chained_ref FROM SOURCE chained_ref_src (REFERENCE "testdrive-chained-ref-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT order_id, (customer).name, ((customer).home_address).city FROM chained_ref
order_id  name     city
----------------------------
1001      Charlie  Springfield

# =============================================================================
# Test 3: Schema with multiple referenced types
# A schema references multiple types from separate schemas
# =============================================================================

# Coordinates schema
$ set coordinates-schema={
    "type": "record",
    "name": "Coordinates",
    "namespace": "com.geo",
    "fields": [
      {"name": "lat", "type": "double"},
      {"name": "lng", "type": "double"}
    ]
  }

$ schema-registry-publish subject=testdrive-coordinates-${testdrive.seed}-value schema-type=avro
\${coordinates-schema}

# Metadata schema
$ set metadata-schema={
    "type": "record",
    "name": "Metadata",
    "namespace": "com.geo",
    "fields": [
      {"name": "source", "type": "string"},
      {"name": "accuracy", "type": "float"}
    ]
  }

$ schema-registry-publish subject=testdrive-metadata-${testdrive.seed}-value schema-type=avro
\${metadata-schema}

# Location schema that uses both Coordinates and Metadata
$ kafka-create-topic topic=multi-ref

$ set location-schema={
    "type": "record",
    "name": "Location",
    "namespace": "com.geo",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "position", "type": "com.geo.Coordinates"},
      {"name": "info", "type": "com.geo.Metadata"}
    ]
  }

$ kafka-ingest format=avro topic=multi-ref schema=${location-schema} references=testdrive-coordinates-${testdrive.seed}-value,testdrive-metadata-${testdrive.seed}-value timestamp=1
{"name": "Coffee Shop", "position": {"lat": 40.7128, "lng": -74.006}, "info": {"source": "GPS", "accuracy": 0.95}}

> BEGIN
> CREATE SOURCE multi_ref_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-multi-ref-${testdrive.seed}')

> CREATE TABLE multi_ref FROM SOURCE multi_ref_src (REFERENCE "testdrive-multi-ref-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT name, (position).lat, (position).lng, (info).source FROM multi_ref
name          lat      lng      source
--------------------------------------
"Coffee Shop" 40.7128  -74.006  GPS

# =============================================================================
# Test 4: Reference with nullable field
# A schema references a type used in a nullable (union) field
# =============================================================================

# Reuse the User schema from Test 1

$ kafka-create-topic topic=nullable-ref

$ set nullable-event-schema={
    "type": "record",
    "name": "NullableEvent",
    "namespace": "com.example",
    "fields": [
      {"name": "event_id", "type": "long"},
      {"name": "user", "type": ["null", "com.example.User"], "default": null}
    ]
  }

$ kafka-ingest format=avro topic=nullable-ref schema=${nullable-event-schema} references=testdrive-user-${testdrive.seed}-value timestamp=1
{"event_id": 1, "user": {"com.example.User": {"id": 200, "name": "Diana"}}}
{"event_id": 2, "user": null}
{"event_id": 3, "user": {"com.example.User": {"id": 201, "name": "Eve"}}}

> BEGIN
> CREATE SOURCE nullable_ref_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-nullable-ref-${testdrive.seed}')

> CREATE TABLE nullable_ref FROM SOURCE nullable_ref_src (REFERENCE "testdrive-nullable-ref-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT event_id, (user).id, (user).name FROM nullable_ref ORDER BY event_id
event_id  id    name
---------------------
1         200   Diana
2         <null> <null>
3         201   Eve

# =============================================================================
# Test 5: Reference in array element type
# A schema references a type used as an array element
# =============================================================================

$ kafka-create-topic topic=array-ref

$ set user-list-schema={
    "type": "record",
    "name": "UserList",
    "namespace": "com.example",
    "fields": [
      {"name": "group_name", "type": "string"},
      {"name": "members", "type": {"type": "array", "items": "com.example.User"}}
    ]
  }

$ kafka-ingest format=avro topic=array-ref schema=${user-list-schema} references=testdrive-user-${testdrive.seed}-value timestamp=1
{"group_name": "Team A", "members": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}

> BEGIN
> CREATE SOURCE array_ref_src
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-array-ref-${testdrive.seed}')

> CREATE TABLE array_ref FROM SOURCE array_ref_src (REFERENCE "testdrive-array-ref-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT group_name, list_length(members) as member_count FROM array_ref
group_name  member_count
-------------------------
"Team A"    2

# =============================================================================
# Cleanup
# =============================================================================

> DROP TABLE IF EXISTS simple_ref;
> DROP TABLE IF EXISTS chained_ref;
> DROP TABLE IF EXISTS multi_ref;
> DROP TABLE IF EXISTS nullable_ref;
> DROP TABLE IF EXISTS array_ref;
> DROP SOURCE IF EXISTS simple_ref_src CASCADE;
> DROP SOURCE IF EXISTS chained_ref_src CASCADE;
> DROP SOURCE IF EXISTS multi_ref_src CASCADE;
> DROP SOURCE IF EXISTS nullable_ref_src CASCADE;
> DROP SOURCE IF EXISTS array_ref_src CASCADE;
