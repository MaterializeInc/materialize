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
# generate-all-updates — generates all batch updates (inserts/deletes)

for batch in 2012-11-29 2012-11-30 2012-12-0{1,2,3,4,5,6,7,8,9} 2012-12-{1,2}{0,1,2,3,4,5,6,7,8,9} 2012-12-3{0,1}
do
    ./generate-batch-update.sh "$batch" >batch_"$batch".sql
done
