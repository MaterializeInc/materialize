#!/usr/bin/env bash
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# deb.sh — deploys official Debian packages in CI.

set -euo pipefail

. misc/shlib/shlib.bash

aws s3 cp \
    "s3://downloads.mtrlz.dev/materialized-$MATERIALIZED_IMAGE_ID-x86_64.deb" \
    ./materialized.deb

curl -F package=@materialized.deb https://"$FURY_APT_PUSH_SECRET"@push.fury.io/materialize
