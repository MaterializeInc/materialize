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

$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "Tyrannosaurus rex"}}}
{"id": 1} {"before": null, "after": {"row": {"id": 1, "creature": "dragon"}}}

# Verify that the errors from before are still there
! SELECT * FROM upsert
contains: Key decode

# Retract the bad key
$ kafka-ingest format=bytes topic=dbzupsert key-format=bytes omit-value=true
broken-key:

# There is still an error, due to the bad value.
! SELECT * FROM upsert
contains: Value error

# Update the bad value
$ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema}
{"id": 2} {"before": null, "after": {"row": {"id": 2, "creature": "cow"}}}

> SELECT * FROM upsert
id creature
-----------
1  dragon
2  cow
