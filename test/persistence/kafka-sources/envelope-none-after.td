# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"}
        ]
    }

> SELECT COUNT(*) FROM envelope_none;
10000

> SELECT SUM(CAST(statistics->'topics'->'testdrive-envelope-none-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) FROM mz_kafka_source_statistics;
0

$ kafka-ingest format=avro topic=envelope-none schema=${schema} publish=true repeat=5000
{"f1": ${kafka-ingest.iteration}}

$ kafka-ingest format=avro topic=envelope-none key-format=avro key-schema=${schema} schema=${schema} publish=true repeat=5000
{"f1": ${kafka-ingest.iteration}} {"f1": ${kafka-ingest.iteration}}

> SELECT COUNT(*), COUNT(DISTINCT f1), MIN(f1), MAX(f1) FROM envelope_none;
20000 5000 0 4999
