# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE SINK upgrade_kafka_sink;
"materialize.public.upgrade_kafka_sink" "CREATE SINK \"materialize\".\"public\".\"upgrade_kafka_sink\" FROM \"materialize\".\"public\".\"static_view\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" TOPIC 'upgrade-kafka-sink' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081/' WITH SNAPSHOT"

$ kafka-verify format=avro sink=materialize.public.upgrade_kafka_sink sort-messages=true
{"before": null, "after": {"row": {"f1": 1}}}
{"before": null, "after": {"row": {"f1": 2}}}
{"before": null, "after": {"row": {"f1": 3}}}
