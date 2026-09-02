# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Companion to glue-schema-registry.td. Exercises the two runtime (decode-time)
# Glue error paths, distinct from the planning-time paths the other scripts
# cover. Moto-only, needs toxiproxy fronting moto for Materialize.

# ---------------------------------------------------------------------------
# Permanent: a record framed with a UUID in no registry. The per-UUID
# writer-schema fetch returns NotFound, cached as a permanent decode error. The
# table's reader schema (by name) resolves fine, only the writer UUID is bogus.
# ---------------------------------------------------------------------------

$ set notfound-schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  }

$ kafka-create-topic topic=notfound partitions=1

# Bytes are valid Avro, but decode never reaches them: the writer-schema lookup
# for this never-registered UUID fails first.
$ kafka-ingest format=avro topic=notfound glue-schema-version-id=00000000-0000-0000-0000-0000000000ff schema=${notfound-schema} timestamp=1
{"a": 1, "b": "x"}

> CREATE SOURCE notfound_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-notfound-${testdrive.seed}')

> CREATE TABLE notfound_table FROM SOURCE notfound_source (REFERENCE "testdrive-notfound-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = 'mz-test-schema')
  ENVELOPE NONE

! SELECT * FROM notfound_table
contains:not found

# ---------------------------------------------------------------------------
# Transient: a record with an uncached but real UUID arrives while the registry
# is unreachable. The fetch error is transient (source halts and retries), not
# a permanent decode error, so the record decodes once connectivity returns.
# Mis-caching it as permanent would hang this script.
#
# The source runs on its own cluster so its halt/retry loop does not churn the
# other sources (which would re-fetch on restart).
# ---------------------------------------------------------------------------

$ set transient-schema={
    "type": "record",
    "name": "transient_row",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "v", "type": "string"}
    ]
  }

# Registered directly against moto (testdrive bypasses toxiproxy), so the UUID
# exists even while Materialize's path to the registry is cut below.
$ glue-create-schema registry=${arg.registry-name} name=mz-transient-schema set-version-id-var=transient-version-id schema=${transient-schema}

$ kafka-create-topic topic=transient partitions=1

> CREATE CLUSTER glue_transient_cluster SIZE 'scale=1,workers=1'

> CREATE SOURCE transient_source
  IN CLUSTER glue_transient_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-transient-${testdrive.seed}')

> CREATE TABLE transient_table FROM SOURCE transient_source (REFERENCE "testdrive-transient-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = 'mz-transient-schema')
  ENVELOPE NONE

> SELECT count(*) FROM transient_table
0

# Cut Materialize's link to the registry.
$ http-request method=POST url=http://toxiproxy:8474/proxies/glue content-type=application/json
{
  "name": "glue",
  "listen": "0.0.0.0:5000",
  "upstream": "moto:5000",
  "enabled": false
}

# Publish a record whose schema UUID the source has not cached yet.
$ kafka-ingest format=avro topic=transient glue-schema-version-id=${transient-version-id} schema=${transient-schema} timestamp=1
{"id": 1, "v": "survives-outage"}

# Confirm the source actually hit the outage (rather than racing past it),
# proving the transient path is exercised, not a lucky post-restore fetch.
> SELECT count(*) > 0 FROM mz_internal.mz_source_statuses
  WHERE name = 'transient_source' AND error IS NOT NULL
true

# Restore connectivity. The retried fetch succeeds and the record decodes.
$ http-request method=POST url=http://toxiproxy:8474/proxies/glue content-type=application/json
{
  "name": "glue",
  "listen": "0.0.0.0:5000",
  "upstream": "moto:5000",
  "enabled": true
}

> SELECT id, v FROM transient_table
1 survives-outage
