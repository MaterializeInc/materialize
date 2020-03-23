# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: compacted kafka logs are produced using the same syntax as
# kafka-avro-console-producer.

$ set keyschema={"type": "string"}

$ set schema=[
    "null",
    {
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }
  ]

$ kafka-create-topic topic=test1

$ kafka-ingest format=avro topic=test1 key-schema=${keyschema} schema=${schema} publish=true timestamp=1
"bird1" {"f1":"goose", "f2": 1}
"birdmore" {"f1":"geese", "f2": 2}
"mammal1" {"f1": "moose", "f2": 1}
"bird1" null
"birdmore" {"f1":"geese", "f2": 56}
"mammalmore" {"f1": "moose", "f2": 42}
"mammal1" null

# TODO: create source in this format. then query to make sure upsert works
# TODO: test key1: null for deletion. test deletion in the middle of a row
