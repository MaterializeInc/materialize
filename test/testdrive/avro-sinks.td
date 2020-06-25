# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test Avro sinks.

# Test that we invent field names for unnamed columns.

> CREATE VIEW unnamed_cols AS SELECT 1, 2 AS b, 3;

> CREATE SINK unnamed_cols_sink FROM unnamed_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'unnamed-cols-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.unnamed_cols_sink
{"before": null, "after": {"column1": 1, "b": 2, "column3": 3}}

# Test that invented field names do not clash with named columns.

> CREATE VIEW clashing_cols AS SELECT 1, 2 AS column1, 3 as b, 4 as b, 5 as b;

> CREATE SINK clashing_cols_sink FROM clashing_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'clashing-cols-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.clashing_cols_sink
{"before": null, "after": {"column1": 1, "column1_1": 2, "b": 3, "b1": 4, "b2": 5}}

# Test a basic sink with multiple rows.

> CREATE VIEW data (a, b) AS VALUES (1, 1), (2, 1), (3, 1), (1, 2)

> CREATE SINK data_sink FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.data_sink
{"before": null, "after": {"a": 1, "b": 1}, "transaction": null}
{"before": null, "after": {"a": 1, "b": 2}, "transaction": null}
{"before": null, "after": {"a": 2, "b": 1}, "transaction": null}
{"before": null, "after": {"a": 3, "b": 1}, "transaction": null}

# Test date/time types.

> CREATE VIEW datetime_data (date, ts, ts_tz) AS VALUES
  (DATE '2000-01-01', TIMESTAMP '2000-01-01 10:10:10.111', TIMESTAMPTZ '2000-01-01 10:10:10.111+02'),
  (DATE '2000-02-01', TIMESTAMP '2000-02-01 10:10:10.111', TIMESTAMPTZ '2000-02-01 10:10:10.111+02')

> CREATE SINK datetime_data_sink FROM datetime_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'datetime-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.datetime_data_sink
{"before": null, "after": {"date": 10988, "ts": 949399810111000, "ts_tz": 949392610111000}, "transaction": null}
{"before": null, "after": {"date": 10957, "ts": 946721410111000, "ts_tz": 946714210111000}, "transaction": null}

> CREATE VIEW time_data (time) AS VALUES (TIME '01:02:03'), (TIME '01:02:04')

> CREATE SINK time_data_sink FROM time_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'time-data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.time_data_sink
{"before": null, "after": {"time": 3723000000}, "transaction": null}
{"before": null, "after": {"time": 3724000000}, "transaction": null}
