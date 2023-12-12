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

{% macro materialize__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    -- The default macro generates a `SELECT * FROM(<query>) WHERE FALSE LIMIT
    -- 0`, which in PostgreSQL is optimized to a no-op that returns no rows. In
    -- Materialize, `SELECT ... LIMIT 0` requires a cluster. Because the
    -- connection is not guaranteed to have a valid cluster, we pass the model
    -- and header statements as raw SQL.
    {% set sql = ({"select_sql": select_sql,"header_sql": select_sql_header}) %}
    {{ return(sql) }}
{% endmacro %}
