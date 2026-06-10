# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "id", "type": "long"}
    ]
  }

$ set schema={
    "type" : "record",
    "name" : "envelope",
    "fields" : [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                  "name": "id",
                  "type": "long"
              },
              {
                "name": "creature",
                "type": "string"
              }]
           },
           "null"
         ]
      },
      {
        "name": "after",
        "type": ["row", "null"]
      }
    ]
  }

# TODO - we should verify here that the topic is not being re-read from the beginning,
# but I don't know of any way to do that in Testdrive today.

$ kafka-ingest format=avro topic=dbzupsert-broken-key key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "Tyrannosaurus rex"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "dragon"}}}

$ kafka-ingest format=avro topic=dbzupsert-broken-value key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "Tyrannosaurus rex"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "dragon"}}}

# Verify that the errors from before are still there
! SELECT * FROM upsert_broken_key_tbl
contains: Key decode

! SELECT * FROM upsert_broken_value_tbl
contains: Value error

# Retract the bad key
$ kafka-ingest format=bytes topic=dbzupsert-broken-key key-format=bytes omit-value=true
broken-key:

> SELECT * FROM upsert_broken_key_tbl
id creature
-----------
1  dragon

# There is still an error in the other source, due to the bad value.
! SELECT * FROM upsert_broken_value_tbl
contains: Value error

# Update the bad value
$ kafka-ingest format=avro topic=dbzupsert-broken-value key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "cow"}}}

> SELECT * FROM upsert_broken_value_tbl
id creature
-----------
1  dragon
2  cow

# Verify that we still can't query the third source, because of the NULL-key error.
# TODO database-issues#8598
# ! select * from upsert_nullkey_tbl
# contains: record with NULL key in UPSERT source

# Ingest a NULL value for our null key, to retract it.
$ kafka-ingest format=bytes topic=upsert-nullkey key-format=bytes key-terminator=:
:

# Now we should be able to query the source.
> select * from upsert_nullkey_tbl
key           text
-------------------
bird1         goose
