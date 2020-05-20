# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1minute

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
      { "name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ avro-ocf-write path=consistency.ocf schema=${consistency}
{"source": "dummy", "partition_count": 1, "partition_id": 0, "timestamp": 0, "offset": 0}
{"source": "${testdrive.temp-dir}/data2.ocf", "partition_count": 1, "partition_id": 0, "timestamp": 1, "offset": 2}

$ avro-ocf-write path=data.ocf schema=${schema}
{"a": 1, "b": 1}
{"a": 2, "b": 2}
{"a": 3, "b": 3}
{"a": 4, "b": 4}

$ avro-ocf-write path=data2.ocf schema=${schema}
{"a": 1, "b": 1}
{"a": 2, "b": 2}

> CREATE MATERIALIZED SOURCE data_byo FROM AVRO OCF '${testdrive.temp-dir}/data.ocf' WITH (consistency='${testdrive.temp-dir}/consistency.ocf', tail=true , max_timestamp_batch_size=2)

> CREATE MATERIALIZED SOURCE data_byo2 FROM AVRO OCF '${testdrive.temp-dir}/data2.ocf' WITH (consistency='${testdrive.temp-dir}/consistency.ocf')

! SELECT * FROM data_byo;
At least one input has no complete timestamps yet.

> SELECT * FROM data_byo2;
a b  line
-------
1  1  1
2  2  2

$ avro-ocf-append path=consistency.ocf
{"source": "${testdrive.temp-dir}/data.ocf", "partition_count": 1, "partition_id": 0, "timestamp": 1, "offset": 2}

> SELECT * FROM data_byo;
a b  line
--------
1  1  1
2  2  2

$ avro-ocf-append path=consistency.ocf
{"source": "${testdrive.temp-dir}/data.ocf", "partition_count": 1, "partition_id": 0, "timestamp": 2, "offset": 4}
{"source": "${testdrive.temp-dir}/data2.ocf", "partition_count": 1, "partition_id": 0, "timestamp": 2, "offset": 4}

$ avro-ocf-append path=data2.ocf
{"a": 3, "b": 3}
{"a": 4, "b": 4}

> SELECT * FROM data_byo;
a b line
-------
1  1  1
2  2  2
3  3  3
4  4  4


> SELECT * FROM data_byo2;
a b  line
-------
1  1  1
2  2  2
