-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{{ config(materialized='materialized_view', cluster='qa_canary_environment_compute', indexes=[{'default': True}]) }}
-- table public_table.table is created directly in mzcompose
SELECT max(c) FROM public_table.table
