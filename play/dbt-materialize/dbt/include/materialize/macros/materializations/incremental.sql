-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{% materialization incremental, adapter='materialize' %}
   -- Todo@jldlaughlin: Fail in a useful way!

   -- TL;DR: dbt-materialize does not support incremental models, use materializedview
   -- models instead.

   -- Longer explanation:
   -- Incremental models are useful because instead of having to rebuild the entire table
   -- in your data warehouse, dbt will only apply your transformations to new data.
   -- Luckily, this is exactly what Materialize's materialized views do! As new data streams
   -- in, Materialize only performs work on that new data. And, all this happens without
   -- extra configurations or scheduled refreshes.
   -- For more information, please visit: https://materialize.com/docs/
{% endmaterialization %}
