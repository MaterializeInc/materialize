#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail; shopt -s nullglob

if [[ ! -f "./Cargo.lock" ]]; then
  echo "Please run this script from the root of the repository."
  exit 1
fi

echo "Killing stray clusterd instances..."
killall clusterd &> /dev/null || true

echo "Resetting db..."
psql -AtX postgres://root@localhost:26257/materialize -c \
  'CREATE DATABASE IF NOT EXISTS materialize;
  DROP SCHEMA IF EXISTS consensus CASCADE;
  CREATE SCHEMA IF NOT EXISTS consensus;
  DROP SCHEMA IF EXISTS tsoracle CASCADE;
  CREATE SCHEMA IF NOT EXISTS tsoracle;
  DROP SCHEMA IF EXISTS storage CASCADE;
  CREATE SCHEMA IF NOT EXISTS storage;'

echo "Resetting mzdata..."
rm -rfv ./mzdata/persist
mkdir -p ./mzdata/

echo "Storing environment-id..."
echo local-az1-a1e5718e-9753-483c-9cb0-fe8f9e715f24-0 > ./mzdata/environment-id
