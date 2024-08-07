# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-from-sql var=expected-compression-implicit-create-sql
SELECT sql FROM expected_compression_implicit_create_sql

> SHOW CREATE SINK compression_implicit;
"materialize.public.compression_implicit" ${expected-compression-implicit-create-sql}

> SHOW CREATE SINK compression_none_explicit;
"materialize.public.compression_none_explicit" "CREATE SINK \"materialize\".\"public\".\"compression_none_explicit\" IN CLUSTER \"quickstart\" FROM \"materialize\".\"public\".\"kafka_sink_from\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'none') FORMAT JSON ENVELOPE DEBEZIUM WITH (PARTITION STRATEGY = 'v0')"

> SHOW CREATE SINK compression_lz4_explicit;
"materialize.public.compression_lz4_explicit" "CREATE SINK \"materialize\".\"public\".\"compression_lz4_explicit\" IN CLUSTER \"quickstart\" FROM \"materialize\".\"public\".\"kafka_sink_from\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'lz4') FORMAT JSON ENVELOPE DEBEZIUM WITH (PARTITION STRATEGY = 'v0')"

> SHOW CREATE SINK compression_gzip_explicit;
"materialize.public.compression_gzip_explicit" "CREATE SINK \"materialize\".\"public\".\"compression_gzip_explicit\" IN CLUSTER \"quickstart\" FROM \"materialize\".\"public\".\"kafka_sink_from\" INTO KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'gzip') FORMAT JSON ENVELOPE DEBEZIUM WITH (PARTITION STRATEGY = 'v0')"
