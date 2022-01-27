# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This REST call will cause toxiproxy to close the connection after processing $TOXIPROXY_BYTES_ALLOWED bytes
# The mzcompose file sets TOXIPROXY_BYTES_ALLOWED to various values so that the interruption
# happens at various stages of S3 processing.

$ http-request method=POST url=http://toxiproxy:8474/proxies/localstack/toxics content-type=application/json
{
  "name": "localstack",
  "type": "limit_data",
  "attributes": { "bytes": ${arg.toxiproxy-bytes-allowed} }
}
