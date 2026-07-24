# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.# Reproducer: NaN-bearing JSONB crashes the JSON sink encoder.

$ set-arg-default default-storage-size=scale=1,workers=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE MATERIALIZED VIEW nan_jsonb_view AS
  SELECT '{"$serde_json::private::Number": "NaN"}'::jsonb AS j;

> SELECT j::text FROM nan_jsonb_view;
"\"NaN\""

> CREATE CLUSTER nan_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK nan_sink
  IN CLUSTER nan_sink_cluster
  FROM nan_jsonb_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-jsonb-nan-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.nan_sink key=false
{"before":null,"after":{"j":"NaN"}}
