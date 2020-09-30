# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Assert required mz_catalog objects are inserted on startup.
> SELECT COUNT(*) FROM mz_tables WHERE global_id LIKE 's%'
13

# There is one entry in mz_indexes for each field_number/expression of the index.
> SELECT COUNT(global_id) FROM mz_indexes WHERE global_id LIKE 's%'
76
