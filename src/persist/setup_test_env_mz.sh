#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Set up the materialize autodeleting s3 bucket env vars. External developers
# will have to use thier own autodeleting bucket and export the same env vars.

export MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mtlz-test-persist-1d-lifecycle-delete"
export AWS_DEFAULT_REGION="us-east-2"
export AWS_PROFILE="mz-scratch-admin"

if [[ "$MZDEV_POSTGRES" ]]; then
  export MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL="$MZDEV_POSTGRES"
fi
