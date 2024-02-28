# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ http-request method=POST url=http://toxiproxy:8474/proxies/kafka_1/toxics content-type=application/json
{
  "name": "kafka_1",
  "type": "timeout",
  "attributes": { "timeout": 0 }
}

$ http-request method=POST url=http://toxiproxy:8474/proxies/kafka_2/toxics content-type=application/json
{
  "name": "kafka_2",
  "type": "timeout",
  "attributes": { "timeout": 0 }
}
