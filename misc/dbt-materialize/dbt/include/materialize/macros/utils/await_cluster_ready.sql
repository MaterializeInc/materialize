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

{% macro await_cluster_ready(cluster, poll_interval=15, lag_threshold='1s') %}

{% for i in range(1, 100000) %}
    {% if is_cluster_ready(cluster, lag_threshold) %}
        {{ return(true) }}
    {% endif %}
    -- Hydration takes time. Be a good
    -- citizen and don't overwhelm mz_catalog_server
    {{ adapter.sleep(poll_interval) }}
{% endfor %}
{{ exceptions.raise_compiler_error("Cluster " ~ deploy_cluster ~ " failed to hydrate within a reasonable amount of time") }}
{% endmacro %}
