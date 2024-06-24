-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

select *
from mz_introspection.mz_compute_frontiers
where to_timestamp(time::text::bigint/1000) < to_timestamp(mz_now()::text::bigint/1000) - INTERVAL '1 minute'
