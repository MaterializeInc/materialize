# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set outfile=${testdrive.data_dir}/billing.protospec

$ compile-protoc source=${testdrive.data_dir}/billing.proto dest=${outfile}

> CREATE SOURCE billing_source FROM 'kafka://${testdrive.kafka-addr}/testdrive-messages-${testdrive.seed}'
  USING SCHEMA '${outfile}'
  WITH (format='protobuf-descriptor', message_name='.Batch')
> CREATE VIEW billing AS SELECT * FROM billing_source

$ kafka-ingest format=protobuf topic=messages message=.Batch timestamp=1
{"id": "2", "interval_start": "2020-01-01 00:00:10", "interval_end": "2020-01-01 00:00:19", "records": [{"interval_start": "2020-01-01 00:00:10", "interval_end": "2020-01-01 00:00:15", "meter": "user", "value": 25, "measurements": [{"resource": "Cpu", "measured_value": 5}, {"resource": "Mem", "measured_value": 128}]}, {"interval_start": "2020-01-01 00:00:16", "interval_end": "2020-01-01 00:00:19", "meter": "user", "value": 125, "measurements": [{"resource": "Cpu", "measured_value": 13}, {"resource": "Mem", "measured_value": 256}]}]}
{"id": "1", "interval_start": "2020-01-01 00:00:00", "interval_end": "2020-01-01 00:00:09", "records": []}

# TODO: default values for enums, strings, bytes do not work right now
$ kafka-ingest format=protobuf topic=messages message=.Batch timestamp=10
{"id": "0", "interval_start": "0", "interval_end": "0", "records": []}

# TODO: these should be fully json
> SELECT * FROM billing
