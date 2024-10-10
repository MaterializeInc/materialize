# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Create a load generator source that has several subsources.
> CREATE SOURCE lga
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (UP TO 100);

> CREATE TABLE accounts FROM SOURCE lga (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE lga (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE lga (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE lga (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE lga (REFERENCE users);

# By default, `SHOW SUBSOURCES` should show all sources in the schema.
> SHOW SUBSOURCES
name           type
-------------------------
lga_progress   progress

> SHOW TABLES
name           comment
-------------------------
accounts       ""
auctions       ""
bids           ""
organizations  ""
users          ""

# Verify the schema filtering by creating two new sources, one in the current
# schema and one in another schema. Verify that `SHOW SUBSOURCES` shows the
# subsources only in the current schema.
> CREATE SCHEMA other
> CREATE SOURCE lgc
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (UP TO 100)
> CREATE SOURCE other.lgo
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (UP TO 100)

> SHOW SUBSOURCES
name           type
-------------------------
lga_progress   progress
lgc_progress   progress

> SHOW TABLES
name           comment
-------------------------
accounts       ""
auctions       ""
bids           ""
organizations  ""
users          ""

> SET SCHEMA = other
> SHOW SUBSOURCES
name           type
-------------------------
lgo_progress   progress

# Verify that you can override the current schema with `FROM ...`.
> SHOW SUBSOURCES FROM public
name           type
-------------------------
lga_progress   progress
lgc_progress   progress

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

> DROP SOURCE other.lgo CASCADE
