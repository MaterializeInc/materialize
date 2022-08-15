# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE MATERIALIZED VIEW static_view (f1) AS VALUES (1),(2),(3);

> CREATE SINK upgrade_kafka_sink FROM static_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'upgrade-kafka-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.upgrade_kafka_sink sort-messages=true
{"before": null, "after": {"row": {"f1": 1}}}
{"before": null, "after": {"row": {"f1": 2}}}
{"before": null, "after": {"row": {"f1": 3}}}
