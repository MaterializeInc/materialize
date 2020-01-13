# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set outfile=${testdrive.data_dir}/simple.protospec

$ compile-protoc source=${testdrive.data_dir}/simple.proto dest=${outfile}

> CREATE SOURCE protomessages FROM 'kafka://${testdrive.kafka-addr}/testdrive-messages-${testdrive.seed}'
  USING SCHEMA '${outfile}'
  WITH (format='protobuf-descriptor', message_name='.Struct')

> CREATE VIEW pm AS SELECT * FROM protomessages
