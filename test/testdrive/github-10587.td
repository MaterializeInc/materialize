# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/10587
#
# This file uses the old create source syntax.

$ set-sql-timeout duration=300s

$ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }

$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ set count=100000

# Create sources and fill them with data and render one dataflow that uses everything just to
# stress the system.

$ kafka-create-topic topic=multi-topic-1

$ kafka-ingest format=avro topic=multi-topic-1 schema=${schema} publish=true repeat=${count}
{"f2": ${kafka-ingest.iteration} }

> CREATE SOURCE s1 FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-multi-topic-1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE NONE;


# Make sure that s1 has been fully timestamped
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

# Now disable retries, and verify that we get the exact same result multiple times
# in a row. Obviously, this test will not always catch the issue since the original
# bug was nondeterministic, but this is a good best-effort smake test.

$ set-max-tries max-tries=1

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000

> SELECT mz_internal.mz_sleep(0.2);
<null>
> SELECT COUNT(*) FROM s1 AS OF AT LEAST 0;
count
-----
100000
