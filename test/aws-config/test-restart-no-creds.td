# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This verifies that materialized can start up even if there are no credentials files available

$ set-sql-timeout duration=1m

# created as a success case in test.td
! SELECT text FROM s3_profile_allowed
contains:Unable to list S3 bucket
