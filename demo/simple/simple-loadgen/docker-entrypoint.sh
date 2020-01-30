#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -euo pipefail

wait-for-it --timeout=60 mysql:3306

cd /loadgen

go run main.go
