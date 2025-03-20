# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that tables with REPLICA IDENTITY DEFAULT or NOTHING will error
# out the source rather than cause wrong data or panics
#
# IDENTITY DEFAULT is the same as USING INDEX (t1_pkey)
#

# MySQL does not have a replica identity
$ nop
