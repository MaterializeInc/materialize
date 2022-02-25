# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "kafka",
  "listen": "0.0.0.0:9093",
  "upstream": "kafka:9091"
}

$ file-append path=dynamic.csv
city,state,zip
Rochester,NY,14618

> CREATE SOURCE input
  FROM FILE '${testdrive.temp-dir}/dynamic.csv'
  WITH (tail=true)
  FORMAT CSV WITH HEADER (city,state,zip)

> CREATE SINK output FROM input
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-byo-sink-${testdrive.seed}'
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ file-append path=dynamic.csv
New York,NY,10004
