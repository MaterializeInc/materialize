# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test Kinesis source functionality

# todo@jldlaughlin: Add a MATERIALIZED VIEW on top of this
# source and drop it. Depends on #2103.
> CREATE SOURCE f
  FROM KINESIS ARN 'arn:aws:kinesis:us-east-2::stream/fake-stream'
  WITH (access_key = 'fake_access_key', secret_access_key = 'fake_secret_access_key')
  FORMAT BYTES;

> DROP SOURCE f

# This tests Materialize's logic around custom AWS regions
! CREATE SOURCE custom_source
  FROM KINESIS ARN 'arn:aws:kinesis:custom-region::stream/fake-stream'
  WITH (access_key = 'fake_access_key', secret_access_key = 'fake_secret_access_key')
  FORMAT BYTES;
If providing a custom region, an `endpoint` option must also be provided

> CREATE SOURCE custom_source
  FROM KINESIS ARN 'arn:aws:kinesis:custom-region::stream/fake-stream'
  WITH (access_key = 'fake_access_key',
        secret_access_key = 'fake_secret_access_key',
        endpoint = 'fake_endpoint')
  FORMAT BYTES;
