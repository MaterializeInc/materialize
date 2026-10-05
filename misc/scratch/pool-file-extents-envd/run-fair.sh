#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Starts environmentd, which builds it first, then runs `park-fair.sh` at TPC-H scale
# factors 10 and 100. Run from the home directory on the scratch instance,
# with this directory's scripts copied there.
set -uo pipefail
cd ~ || exit 1
echo "$(date) sf10 start"
./envd-start.sh || exit 1
./setup-tpch.sh 10
./mkempty.sh
./park-fair.sh f10 "8 4" 32
echo "$(date) sf10 done"
pkill -x environmentd; pkill -x clusterd; sleep 10
./envd-start.sh || exit 1
echo "$(date) sf100 setup"
./setup-tpch.sh 100
./mkempty.sh
SWAP_GIB=200 ./park-fair.sh f100 "8"
echo "$(date) sf100 done"
echo RUNALLDONE
