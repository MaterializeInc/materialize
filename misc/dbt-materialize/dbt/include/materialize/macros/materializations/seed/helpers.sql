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

{% macro materialize__reset_csv_table(model, full_refresh, old_relation, agate_table, cluster) %}
    {% set sql = "" %}
    -- Allow setting a cluster configuration for seeds in `dbt_project.yml` or
    -- a .yml file in the seed target path. If no cluster is configured, use
    -- the target cluster from `profiles.yml`. If none exists, fall back to the
    -- default cluster configured for the connected user.
    {%- set cluster = model['config'].get('cluster', target.cluster) -%}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
    {% else %}
        {{ truncate_relation_sql(old_relation, cluster) }}
        {% set sql = "delete from " ~ old_relation.render() %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}
