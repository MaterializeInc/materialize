# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-ingest topic=largeinput format=bytes repeat=11000000
A,B,0

# Long enough to ingest all the data
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="60s"
<null>

> SELECT count(*) FROM largeinput_tbl
11000001

> CREATE SINK output FROM largeinput_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-byo-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
