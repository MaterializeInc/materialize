# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ensure we actually ingested all the data so far

# Could be run when storaged is down
> SET transaction_isolation = serializable

> SELECT * from topic
data         offset
-----------------------------------
one          0
two          1
three        2
