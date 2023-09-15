# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests that assert the privileges that are assumed to be always granted to #
# the mz_support user. This test can be rewritten to validate the output of the
# `connection=mz_support` command once we have `SET ROLE` working.

$ postgres-connect name=mz_support url=postgres://mz_support:materialize@${testdrive.materialize-internal-sql-addr}

> CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES WITH (size = '1');

# The mz_support user can list database sources.
$ postgres-execute connection=mz_support
SHOW SOURCEs;

# The mz_support user can execute `SHOW CREATE ...` commands.
$ postgres-execute connection=mz_support
SHOW CREATE SOURCE bids;

# The mz_support user can execute `EXPLAIN PLAN ...` commands.
$ postgres-execute connection=mz_support
EXPLAIN OPTIMIZED PLAN FOR SELECT * FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user can execute `EXPLAIN TIMESTAMP ...` commands.
$ postgres-execute connection=mz_support
EXPLAIN TIMESTAMP FOR SELECT * FROM bids b JOIN users u ON(b.buyer = u.id);

# The mz_support user cannot execute `SELECT ...` commands.
# We can uncomment this test once all regular commands are executed from `mz_support`
# ! SELECT * FROM bids
# contains:permission denied for SOURCE "materialize.public.bids"
