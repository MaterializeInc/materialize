# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM data
1

# This next query will fail if a bug like #10234 occurs again, where a migration
# injects a `WITH` option into the wrong spot.

> SHOW CREATE SOURCE data
"materialize.public.data" "CREATE SOURCE \"materialize\".\"public\".\"data\" FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION \"materialize\".\"public\".\"csr_conn\" SEED VALUE SCHEMA '{\"type\":\"record\",\"name\":\"foo\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}' ENVELOPE NONE"
