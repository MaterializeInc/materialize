# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#

# Basic test for `TEST SCRIPT` sources.

# replace timestamps
$ set-regex match=(\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>


> CREATE CONNECTION c_conn
  FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}'

> CREATE SOURCE unit
  FROM TEST SCRIPT
  '[
    {"command": "emit", "key": "fish", "value": "value", "offset": 0},
    {"command": "emit", "key": "fish2", "value": "hmm", "offset": 1},
    {"command": "emit" ,"key": "fish", "value": "value2", "offset": 2}
  ]'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> SELECT * from unit
key           data
--------------------
fish          value2
fish2         hmm

> EXPLAIN TIMESTAMP FOR SELECT * FROM unit
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.unit (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

> CREATE SOURCE unit_terminated
  FROM TEST SCRIPT
  '[
    {"command": "emit", "key": "fish", "value": "value", "offset": 0},
    {"command": "terminate"}
  ]'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
  ENVELOPE UPSERT

> SELECT * from unit_terminated
key           data
--------------------
fish          value

# Terminal sources have empty uppers
> EXPLAIN TIMESTAMP FOR SELECT * FROM unit_terminated
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <>\n                          upper:[]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.unit_terminated (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[]\n"

# Query automatically generated progress topics
> SELECT * FROM unit_progress
3

> SELECT * FROM unit_terminated_progress

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>

> EXPLAIN TIMESTAMP FOR SELECT * FROM unit_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.unit_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

> EXPLAIN TIMESTAMP FOR SELECT * FROM unit_terminated_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <>\n                          upper:[]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.unit_terminated_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[]\n"
