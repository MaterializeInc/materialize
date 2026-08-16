-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Drop everything created by the scaffold and any loaded domains.
-- Idempotent: safe to run repeatedly, safe to run when only some domains
-- were loaded. Order matters: drop `empty CASCADE` first to kill the
-- moments chain and everything derived from it, then drop the static
-- lookup tables that don't transitively depend on `empty`.

-- Scaffold + everything reachable from a moment.
DROP TABLE IF EXISTS empty CASCADE;

-- Static lookups (have no dep on `empty`, must be dropped explicitly).
DROP VIEW IF EXISTS people    CASCADE;  -- common
DROP VIEW IF EXISTS items     CASCADE;  -- auctions
DROP VIEW IF EXISTS products  CASCADE;  -- ecommerce
DROP VIEW IF EXISTS accounts  CASCADE;  -- banking
DROP VIEW IF EXISTS devices   CASCADE;  -- iot
