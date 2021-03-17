# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set consistency={
     "name": "materialize.byo.consistency",
     "type": "record",
     "fields": [
         {
           "name": "source",
           "type": "string"
         },
         {
           "name": "partition_count",
           "type": "int"
         },
         {
           "name": "partition_id",
           "type": ["int","string"]
         },
         {
            "name": "timestamp",
            "type": "long"
         },
         {
           "name": "offset",
           "type": "long"
         }
     ]
  }

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-ingest format=avro topic=input-consistency schema=${consistency} timestamp=2
{"source": "testdrive-input-${testdrive.seed}", "partition_count": 1, "partition_id": {"int": 0}, "timestamp": 52, "offset": 10}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=14
{"a": 4, "b": 1}
{"a": 5, "b": 2}

> SELECT * FROM input_byo
a  b
------
1   1
2   1
3   1
1   2
11  11
22  11
3   4
5   6
4  1
5  2

$ kafka-verify format=avro sink=materialize.public.output
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "10"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "10"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "10"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "10"}}
{"before": null, "after": {"row": {"a": 11, "b": 11}}, "transaction": {"id": "30"}}
{"before": null, "after": {"row": {"a": 22, "b": 11}}, "transaction": {"id": "30"}}
{"before": null, "after": {"row": {"a": 3, "b": 4}}, "transaction": {"id": "40"}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}, "transaction": {"id": "40"}}
{"before": null, "after": {"row": {"a": 4, "b": 1}}, "transaction": {"id": "52"}}
{"before": null, "after": {"row": {"a": 5, "b": 2}}, "transaction": {"id": "52"}}
