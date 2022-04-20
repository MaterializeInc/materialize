# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ avro-ocf-write path=avro_ocf_source.ocf schema=${writer-schema}
{"a": 1, "b": 2}
{"a": 3, "b": 4}

> CREATE MATERIALIZED SOURCE avro_ocf_source FROM AVRO OCF '${testdrive.temp-dir}/avro_ocf_source.ocf' WITH (tail = true)

> SELECT COUNT(*) > 0 FROM avro_ocf_source;
true

$ avro-ocf-append path=avro_ocf_source.ocf
{"a": 5, "b": 6}
{"a": 7, "b": 8}
