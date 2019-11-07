#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -euxo pipefail

mkswap /dev/nvme0n1
mkfs.ext4 /dev/nvme1n1

