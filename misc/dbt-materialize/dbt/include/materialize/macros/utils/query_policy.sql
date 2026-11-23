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

{# Only absence of the catalog means an older server. Query and permission
   errors must propagate so a deployment cannot silently lose its policy. #}
{% macro internal_get_cluster_query_policy(cluster_name) %}
    {% set catalog_relation_exists %}
        SELECT count(*) > 0
        FROM mz_catalog.mz_objects o
        JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
        WHERE s.name = 'mz_internal' AND o.name = 'mz_query_policy_attachments'
    {% endset %}
    {% set exists_result = run_query(catalog_relation_exists) %}
    {% if not execute or not exists_result.rows[0][0] %}
        {{ return(none) }}
    {% endif %}

    {% set policy_query %}
        SELECT p.name
        FROM mz_internal.mz_query_policy_attachments a
        JOIN mz_catalog.mz_clusters c ON c.id = a.object_id
        LEFT JOIN mz_internal.mz_query_policies p ON p.id = a.policy_id
        WHERE a.object_type = 'cluster'
          AND c.name = {{ dbt.string_literal(cluster_name) }}
    {% endset %}
    {% set policies = run_query(policy_query) %}
    {% if policies.rows | length == 0 %}
        {{ return(none) }}
    {% endif %}
    {% if policies.rows | length != 1 or policies.rows[0][0] is none %}
        {{ exceptions.raise_compiler_error("Cannot resolve query policy for cluster " ~ cluster_name) }}
    {% endif %}
    {{ return(policies.rows[0][0]) }}
{% endmacro %}
