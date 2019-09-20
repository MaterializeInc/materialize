#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

for tag in latest "$(date +%Y-%m-%d)" ; do
    docker push materialize/mzcli:"$tag"
done
