# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

>[version<2600700] SHOW CREATE SINK compression_implicit;
materialize.public.compression_implicit "CREATE SINK materialize.public.compression_implicit IN CLUSTER quickstart FROM materialize.public.kafka_sink_from INTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink') FORMAT JSON ENVELOPE DEBEZIUM;"

>[version>=2600700] SHOW CREATE SINK compression_implicit;
materialize.public.compression_implicit "CREATE SINK materialize.public.compression_implicit\nIN CLUSTER quickstart\nFROM materialize.public.kafka_sink_from\nINTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink')\nFORMAT JSON\nENVELOPE DEBEZIUM;"

>[version<2600700] SHOW CREATE SINK compression_none_explicit;
"materialize.public.compression_none_explicit" "CREATE SINK materialize.public.compression_none_explicit IN CLUSTER quickstart FROM materialize.public.kafka_sink_from INTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'none') FORMAT JSON ENVELOPE DEBEZIUM;"

>[version>=2600700] SHOW CREATE SINK compression_none_explicit;
"materialize.public.compression_none_explicit" "CREATE SINK materialize.public.compression_none_explicit\nIN CLUSTER quickstart\nFROM materialize.public.kafka_sink_from\nINTO\n    KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'none')\nFORMAT JSON\nENVELOPE DEBEZIUM;"

>[version<2600700] SHOW CREATE SINK compression_lz4_explicit;
"materialize.public.compression_lz4_explicit" "CREATE SINK materialize.public.compression_lz4_explicit IN CLUSTER quickstart FROM materialize.public.kafka_sink_from INTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'lz4') FORMAT JSON ENVELOPE DEBEZIUM;"

>[version>=2600700] SHOW CREATE SINK compression_lz4_explicit;
"materialize.public.compression_lz4_explicit" "CREATE SINK materialize.public.compression_lz4_explicit\nIN CLUSTER quickstart\nFROM materialize.public.kafka_sink_from\nINTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'lz4')\nFORMAT JSON\nENVELOPE DEBEZIUM;"

>[version<2600700] SHOW CREATE SINK compression_gzip_explicit;
"materialize.public.compression_gzip_explicit" "CREATE SINK materialize.public.compression_gzip_explicit IN CLUSTER quickstart FROM materialize.public.kafka_sink_from INTO KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'gzip') FORMAT JSON ENVELOPE DEBEZIUM;"

>[version>=2600700] SHOW CREATE SINK compression_gzip_explicit;
"materialize.public.compression_gzip_explicit" "CREATE SINK materialize.public.compression_gzip_explicit\nIN CLUSTER quickstart\nFROM materialize.public.kafka_sink_from\nINTO\n    KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'kafka-sink', COMPRESSION TYPE = 'gzip')\nFORMAT JSON\nENVELOPE DEBEZIUM;"
