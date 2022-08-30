# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> SHOW CREATE SINK upgrade_kafka_sink;
"materialize.public.upgrade_kafka_sink" "CREATE SINK \"materialize\".\"public\".\"upgrade_kafka_sink\" FROM \"materialize\".\"public\".\"static_view\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" TOPIC 'upgrade-kafka-sink' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \"materialize\".\"public\".\"csr_conn\" WITH SNAPSHOT"
