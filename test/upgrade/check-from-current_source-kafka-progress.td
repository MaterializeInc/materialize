# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ensure progress sources automatically created
$ set-regex match=\d+ replacement=<NUMBER>
> SELECT partition::text, "offset" FROM kafka_source_progress
(<NUMBER>,) <NUMBER>
[<NUMBER>,<NUMBER>] <NUMBER>

> SHOW CREATE SOURCE kafka_source
materialize.public.kafka_source "CREATE SOURCE \"materialize\".\"public\".\"kafka_source\" FROM KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'testdrive-upgrade-kafka-source-current_source-<NUMBER>') FORMAT AVRO USING SCHEMA '{   \"type\": \"record\",   \"name\": \"cpx\",   \"fields\": [     {\"name\": \"a\", \"type\": \"long\"},     {\"name\": \"b\", \"type\": \"long\"}   ] }' ENVELOPE NONE EXPOSE PROGRESS AS \"materialize\".\"public\".\"kafka_source_progress\""
