# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# This test exercises dates and times at the boundary (e.g., by sending them
# through pgwire). Operations on dates and times are more thoroughly tested in
# dates-times.slt.

$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "d",
        "type": {
          "type": "int",
          "logicalType": "date"
        }
      },
      {
        "name": "ts_millis",
        "type": {
          "type": "long",
          "logicalType": "timestamp-millis"
        }
      },
      {
        "name": "ts_micros",
        "type": {
          "type": "long",
          "logicalType": "timestamp-micros"
        }
      }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"d": 18135, "ts_millis": 1566865029000000, "ts_micros": 1566865029000000}
{"d": 0, "ts_millis": 65000000, "ts_micros": 65000000}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> BEGIN
> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')

> CREATE TABLE data_tbl FROM SOURCE data (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
> COMMIT

> CREATE MATERIALIZED VIEW data_view as SELECT * from data_tbl

> SELECT * FROM data_view
1970-01-01 "1970-01-01 18:03:20" "1970-01-01 00:01:05"
2019-08-27 "+51621-12-22 21:50:00" "2019-08-27 00:17:09"

> CREATE SINK data_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM data_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-roundtrip-${testdrive.seed}')
  KEY (d) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-data-roundtrip-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}},{"name":"ts_millis","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ts_micros","type":{"type":"long","logicalType":"timestamp-micros"}}]}

$ kafka-verify-data format=avro sink=materialize.public.data_sink sort-messages=true
{"d": 0} {"d": 0, "ts_millis": 65000000, "ts_micros": 65000000}
{"d": 18135} {"d": 18135, "ts_millis": 1566865029000000, "ts_micros": 1566865029000000}

> CREATE MATERIALIZED VIEW ts_precision AS
  SELECT
    TRUE AS key,
    '1970-01-01T00:00:00.123456'::timestamp AS ts,
    '1970-01-01T00:00:00.123456'::timestamp(0) AS ts0,
    '1970-01-01T00:00:00.123456'::timestamp(1) AS ts1,
    '1970-01-01T00:00:00.123456'::timestamp(2) AS ts2,
    '1970-01-01T00:00:00.123456'::timestamp(3) AS ts3,
    '1970-01-01T00:00:00.123456'::timestamp(4) AS ts4,
    '1970-01-01T00:00:00.123456'::timestamp(5) AS ts5,
    '1970-01-01T00:00:00.123456'::timestamp(6) AS ts6,
    '1970-01-01T00:00:00.123456'::timestamptz AS tstz,
    '1970-01-01T00:00:00.123456'::timestamptz(0) AS tstz0,
    '1970-01-01T00:00:00.123456'::timestamptz(1) AS tstz1,
    '1970-01-01T00:00:00.123456'::timestamptz(2) AS tstz2,
    '1970-01-01T00:00:00.123456'::timestamptz(3) AS tstz3,
    '1970-01-01T00:00:00.123456'::timestamptz(4) AS tstz4,
    '1970-01-01T00:00:00.123456'::timestamptz(5) AS tstz5,
    '1970-01-01T00:00:00.123456'::timestamptz(6) AS tstz6

> CREATE SINK ts_precision_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM ts_precision
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-ts-precision-${testdrive.seed}')
  KEY (key) NOT ENFORCED
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

$ schema-registry-verify schema-type=avro subject=testdrive-ts-precision-${testdrive.seed}-value
{"type":"record","name":"envelope","fields":[{"name":"key","type":"boolean"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"ts0","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ts1","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ts2","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ts3","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ts4","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"ts5","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"ts6","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"tstz","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"tstz0","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"tstz1","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"tstz2","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"tstz3","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"tstz4","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"tstz5","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"tstz6","type":{"type":"long","logicalType":"timestamp-micros"}}]}

$ kafka-verify-data format=avro sink=materialize.public.ts_precision_sink sort-messages=true
{"key": true} {"key": true, "ts": 123456, "ts0": 0, "ts1": 100, "ts2": 120, "ts3": 123, "ts4": 123500, "ts5": 123460, "ts6": 123456, "tstz": 123456, "tstz0": 0, "tstz1": 100, "tstz2": 120, "tstz3": 123, "tstz4": 123500, "tstz5": 123460, "tstz6": 123456}

! SELECT '1970-01-01T00:00:00.123456'::timestamp(-1)
contains:precision for type timestamp or timestamptz must be between 0 and 6

! SELECT '1970-01-01T00:00:00.123456'::timestamp(7)
contains:precision for type timestamp or timestamptz must be between 0 and 6

> SELECT INTERVAL '1' SECOND
"00:00:01"

> SELECT INTERVAL '2' MINUTE
"00:02:00"

> SELECT INTERVAL '3' HOUR
"03:00:00"

> SELECT INTERVAL '1' DAY
"1 day"

> SELECT INTERVAL '6' MONTH
"6 months"

> SELECT INTERVAL '10' YEAR
"10 years"

> SELECT TIMESTAMP WITH TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 06:10:10.410 UTC"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+07:00'
"1989-06-01 02:10:10.410 UTC"

# microseconds are returned with nanosecond precision in the binary format
> SELECT TIMESTAMPTZ '1989-06-01 06:10:10.12345678+00:00'
"1989-06-01 06:10:10.123457 UTC"

> SELECT TIMESTAMP WITHOUT TIME ZONE '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> SELECT TIMESTAMP '1989-06-01 10:10:10.410+04:00'
"1989-06-01 10:10:10.410"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+7'
"1989-06-01 02:10:10.410 UTC"

> SELECT TIMESTAMPTZ '1989-06-01 9:10:10.410+700'
"1989-06-01 02:10:10.410 UTC"

> SELECT '1989-06-01 10:10:10.410+04:00'::timestamptz::text
"1989-06-01 06:10:10.41+00"

> SELECT '1989-06-01 10:10:10.413+04:00'::timestamptz::text
"1989-06-01 06:10:10.413+00"

# The text format should only ever return microseconds
> SELECT '1989-06-01 10:10:10.12345678+04:00'::timestamptz::text
"1989-06-01 06:10:10.123457+00"

> SELECT '1989-06-01 10:10:10.1234564+04:00'::timestamptz::text
"1989-06-01 06:10:10.123456+00"

# Timestamp columns with precisions
> DROP TABLE IF EXISTS temporal_types;

> CREATE TABLE temporal_types (timestamp_col TIMESTAMP, timestamptz_col TIMESTAMPTZ, timestamp_prec_col TIMESTAMP(3), timestamptz_prec_col TIMESTAMPTZ(1));

> INSERT INTO temporal_types VALUES ('2010-10-10 10:10:10.123456789+00','2010-10-10 10:10:10.123456789+00', '2010-10-10 10:10:10.123456789+00','2010-10-10 10:10:10.123456789+00');

> SELECT * FROM temporal_types;
"2010-10-10 10:10:10.123457" "2010-10-10 10:10:10.123457 UTC" "2010-10-10 10:10:10.123" "2010-10-10 10:10:10.100 UTC"
