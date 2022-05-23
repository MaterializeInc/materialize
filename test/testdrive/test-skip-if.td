
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ skip-if
SELECT false;

# false skip result means we run this and see the failure
! SELECT nonsense;
contains:column "nonsense" does not exist

$ skip-if
SELECT true;

# true skip result means we don't run this erroneous statement
> SELECT nonsense;
