# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE SOURCE protomessages FROM 'kafka://${testdrive.kafka-addr}/testdrive-messages-${testdrive.seed}'
  USING SCHEMA '${testdrive.protobuf-descriptors}'
  WITH (format = 'protobuf-descriptor', message_name = '.Struct')

> CREATE VIEW pm AS SELECT * FROM protomessages

> CREATE SOURCE protomessages2 FROM 'kafka://${testdrive.kafka-addr}/testdrive-messages-${testdrive.seed}'
  USING SCHEMA FILE '${testdrive.protobuf-descriptors-file}'
  WITH (format = 'protobuf-descriptor', message_name = '.Struct')

> CREATE VIEW pm2 AS SELECT * FROM protomessages

$ kafka-ingest format=protobuf topic=messages message=.Struct timestamp=1
{"int": 1, "bad_int": 1, "bin": "One", "st": "my-string"}
{"int": 2, "bad_int": 2, "bin": "One", "st": "something-valid"}

# TODO: default values for enums, strings, bytes do not work right now
$ kafka-ingest format=protobuf topic=messages message=.Struct timestamp=10
{"int": 0, "bad_int": 0, "bin": "Zero", "st": ""}

# TODO: these should be fully json
> SELECT * FROM pm
1 1 ONE  my-string
2 2 ONE  something-valid
