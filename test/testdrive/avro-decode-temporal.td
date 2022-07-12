# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# 'date' is days since the start of the UNiX epoch

$ set date={"type": "record", "name": "date_field", "fields": [ { "name": "f1", "type": { "logicalType": "date", "type": "int" } } ] }

$ kafka-create-topic topic=avro-decode-date

$ kafka-ingest format=avro topic=avro-decode-date schema=${date} timestamp=1
{"f1": -1}
{"f1": 0}
{"f1": 1}
{"f1": 12345678}

> CREATE SOURCE avro_decode_date
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-date-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${date}'
  ENVELOPE NONE

> SELECT * FROM avro_decode_date
1969-12-31
1970-01-01
1970-01-02
+35771-04-27

# Time and time-millis do not appear to be decoded to a temporal data type, see rc/avro/tests/schema.rs

$ set time-millis={"type": "record", "name": "time_millis_field", "fields": [ { "name": "f1", "type": { "logicalType": "time-millis", "type": "int" } } ] }

$ kafka-create-topic topic=avro-decode-time-millis

$ kafka-ingest format=avro topic=avro-decode-time-millis schema=${time-millis} timestamp=1
{"f1": -10}
{"f1": 0}
{"f1": 1}
{"f1": 12345678}

> CREATE SOURCE avro_decode_time_millis
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-date-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${time-millis}'
  ENVELOPE NONE

> SELECT * FROM avro_decode_time_millis
-1
0
1
12345678

#
# timestamp-millis
#

$ set timestamp-millis={"type": "record", "name": "timestamp_millis_field", "fields": [ { "name": "f1", "type": { "logicalType": "timestamp-millis", "type": "long" } } ] }

$ kafka-create-topic topic=avro-decode-timestamp-millis

$ kafka-ingest format=avro topic=avro-decode-timestamp-millis schema=${timestamp-millis} timestamp=1
{"f1": 0}
{"f1": 1}
{"f1": 10}
{"f1": 100}
{"f1": 1000}
{"f1": 10000}
{"f1": 61000}
{"f1": 1234567890}

> CREATE SOURCE avro_decode_timestamp_millis
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-timestamp-millis-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${timestamp-millis}'
  ENVELOPE NONE

> SELECT * FROM avro_decode_timestamp_millis
"1970-01-01 00:00:00"
"1970-01-01 00:00:00.001"
"1970-01-01 00:00:00.010"
"1970-01-01 00:00:00.100"
"1970-01-01 00:00:01"
"1970-01-01 00:00:10"
"1970-01-01 00:01:01"
"1970-01-15 06:56:07.890"

#
# timestamp-micros
#

$ set timestamp-micros={"type": "record", "name": "timestamp_micros_field", "fields": [ { "name": "f1", "type": { "logicalType": "timestamp-micros", "type": "long" } } ] }

$ kafka-create-topic topic=avro-decode-timestamp-micros

$ kafka-ingest format=avro topic=avro-decode-timestamp-micros schema=${timestamp-micros} timestamp=1
{"f1": 0}
{"f1": 1}
{"f1": 10}
{"f1": 100}
{"f1": 1000}
{"f1": 10000}
{"f1": 61000000}
{"f1": 1234567890}

> CREATE SOURCE avro_decode_timestamp_micros
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-timestamp-micros-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${timestamp-micros}'
  ENVELOPE NONE

> SELECT * FROM avro_decode_timestamp_micros
"1970-01-01 00:00:00"
"1970-01-01 00:00:00.000001"
"1970-01-01 00:00:00.000010"
"1970-01-01 00:00:00.000100"
"1970-01-01 00:00:00.001"
"1970-01-01 00:00:00.010"
"1970-01-01 00:01:01"
"1970-01-01 00:20:34.567890"

#
# local-timestamp-millis is not decoded to a temporal type
#

$ set local-timestamp-millis={"type": "record", "name": "timestamp_millis_field", "fields": [ { "name": "f1", "type": { "logicalType": "local-timestamp-millis", "type": "long" } } ] }

$ kafka-create-topic topic=avro-decode-local-timestamp-millis

$ kafka-ingest format=avro topic=avro-decode-local-timestamp-millis schema=${local-timestamp-millis} timestamp=1
{"f1": 0}
{"f1": 1}
{"f1": 10}
{"f1": 100}
{"f1": 1000}
{"f1": 10000}
{"f1": 1234567890}

> CREATE SOURCE avro_decode_local_timestamp_millis
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-local-timestamp-millis-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${local-timestamp-millis}'
  ENVELOPE NONE

> SELECT * FROM avro_decode_local_timestamp_millis
0
1
10
100
1000
10000
1234567890

#
# duration is not tested because there is no support for "fixed"
#
