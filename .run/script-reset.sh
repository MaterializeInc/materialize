#!/usr/bin/env bash

set -euo pipefail; shopt -s nullglob

if [[ ! -d "./.run" ]]; then
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
