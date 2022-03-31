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

{% materialization index, adapter='materialize' %}
   -- TL;DR: dbt-materialize does not support incremental models, use materializedview

    {{ exceptions.raise_compiler_error(
        """
        The dbt-materialize index custom materialization has been deprecated in favor
        of native support on view, materializedview, and source creation.

        See: https://materialize.com/docs/sql/create-index for more information about creating indexes
        with materialize.
        See: TODO: https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#materializations
        See: TODO: https://docs.getdbt.com/reference/resource-configs/postgres-configs but for Materialize.
        """
    )}}
{% endmaterialization %}
