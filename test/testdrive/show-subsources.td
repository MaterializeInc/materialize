# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Create a load generator source that has several subsources.
> CREATE SOURCE lga FROM LOAD GENERATOR AUCTION FOR ALL TABLES

# By default, `SHOW SUBSOURCES` should show all sources in the schema.
> SHOW SUBSOURCES
name           type
-------------------------
accounts       subsource
auctions       subsource
bids           subsource
lga_progress   progress
organizations  subsource
users          subsource

# Verify the schema filtering by creating two new sources, one in the current
# schema and one in another schema. Verify that `SHOW SUBSOURCES` shows the
# subsources only in the current schema.
> CREATE SCHEMA other
> CREATE SOURCE lgc FROM LOAD GENERATOR COUNTER
> CREATE SOURCE other.lgo FROM LOAD GENERATOR COUNTER

> SHOW SUBSOURCES
name           type
-------------------------
accounts       subsource
auctions       subsource
bids           subsource
lga_progress   progress
lgc_progress   progress
organizations  subsource
users          subsource

> SET SCHEMA = other
> SHOW SUBSOURCES
name           type
-------------------------
lgo_progress   progress

# Verify that you can override the current schema with `FROM ...`.
> SHOW SUBSOURCES FROM public
name           type
-------------------------
accounts       subsource
auctions       subsource
bids           subsource
lga_progress   progress
lgc_progress   progress
organizations  subsource
users          subsource

# Verify that `ON ...` filters to the subsources of the named source.
> SHOW SUBSOURCES ON lgo
name           type
-------------------------
lgo_progress   progress

# Verify again with a cross-schema reference.
> SHOW SUBSOURCES ON public.lgc
name           type
-------------------------
lgc_progress   progress

# Verify that you cannot combine a schema filter with a source filter.
! SHOW SUBSOURCES FROM public ON lga
contains:Cannot specify both FROM and ON

# Verify that `ON` validates that the referenced object is a source.
> CREATE TABLE t (a int)
! SHOW SUBSOURCES ON t
contains:cannot show subsources on materialize.other.t because it is a table
