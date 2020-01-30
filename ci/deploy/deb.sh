#!/usr/bin/env bash
# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# deb.sh — deploys official Debian packages in CI.

set -euo pipefail

. misc/shlib/shlib.bash

aws s3 cp \
    "s3://downloads.mtrls.dev/materialized-$MATERIALIZED_IMAGE_ID-x86_64.deb" \
    ./materialized.deb

curl -F package=@materialized.deb https://"$FURY_APT_PUSH_SECRET"@push.fury.io/materialize
