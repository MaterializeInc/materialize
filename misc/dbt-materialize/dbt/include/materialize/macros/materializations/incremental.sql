-- Copyright 2020 Josh Wills. All rights reserved.
-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{% materialization incremental, adapter='materialize' %}
   -- TL;DR: dbt-materialize does not support incremental models, use materialized_view
   -- models instead.

   -- Longer explanation:
   -- Incremental models are useful because instead of having to rebuild the entire table
   -- in your data warehouse, dbt will only apply your transformations to new data.
   -- Luckily, this is exactly what Materialize's materialized views do! As new data streams
   -- in, Materialize only performs work on that new data. And, all this happens without
   -- extra configurations or scheduled refreshes.
   -- For more information, please visit: https://materialize.com/docs/
    {{ exceptions.CompilationError(
        """
        dbt-materialize does not support incremental models, because all views in
        Materialize are natively maintained incrementally.

        Use the `materialized_view` custom materialization instead.

        See: https://materialize.com/docs/overview/api-components/#materialized-views
        See: https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#materializations
        """
    )}}
{% endmaterialization %}
