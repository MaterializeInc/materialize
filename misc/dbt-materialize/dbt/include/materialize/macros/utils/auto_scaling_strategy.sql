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

{#
Helpers for the `AUTO SCALING STRATEGY` cluster option (hydration burst),
which lets a managed cluster temporarily burst to a larger size while it has
un-hydrated objects, then return to its steady size once hydration completes.

The adapter models the strategy as a nested mapping:

  auto_scaling_strategy:
    on_hydration:
      hydration_size: '800cc'   # required; must differ from the cluster SIZE
      linger_duration: '15s'    # optional; default '0s'

An empty mapping ({}) renders as `AUTO SCALING STRATEGY = ()`, which disables
autoscaling.
#}

{#
Validates the shape of an `auto_scaling_strategy` mapping and its
compatibility with the cluster's SIZE and schedule. Raises a compiler error
on invalid input. `size` and `schedule_type` are optional and only checked
when provided.
#}
{% macro internal_validate_auto_scaling_strategy(auto_scaling_strategy, size=none, schedule_type=none) %}
    {% if auto_scaling_strategy is not mapping %}
        {{ exceptions.raise_compiler_error(
            "auto_scaling_strategy must be a mapping, e.g. "
            ~ "{'on_hydration': {'hydration_size': '800cc', 'linger_duration': '15s'}}. "
            ~ "Got: " ~ auto_scaling_strategy
        ) }}
    {% endif %}

    {# AUTO SCALING STRATEGY is only supported on clusters with the default
       MANUAL schedule, matching the server-side restriction. #}
    {% if schedule_type is not none and schedule_type != 'manual' %}
        {{ exceptions.raise_compiler_error(
            "auto_scaling_strategy cannot be combined with a schedule_type "
            ~ "other than 'manual'. Got schedule_type: " ~ schedule_type
        ) }}
    {% endif %}

    {% for key in auto_scaling_strategy %}
        {% if key != 'on_hydration' %}
            {{ exceptions.raise_compiler_error(
                "Unknown auto_scaling_strategy key '" ~ key ~ "'. "
                ~ "The only supported sub-policy is 'on_hydration'."
            ) }}
        {% endif %}
    {% endfor %}

    {% if auto_scaling_strategy %}
        {% set on_hydration = auto_scaling_strategy.get('on_hydration') %}
        {% if on_hydration is not mapping or not on_hydration.get('hydration_size') %}
            {{ exceptions.raise_compiler_error(
                "auto_scaling_strategy.on_hydration must be a mapping with a "
                ~ "'hydration_size' key, e.g. {'hydration_size': '800cc', "
                ~ "'linger_duration': '15s'}. Got: " ~ on_hydration
            ) }}
        {% endif %}
        {% for key in on_hydration %}
            {% if key not in ['hydration_size', 'linger_duration'] %}
                {{ exceptions.raise_compiler_error(
                    "Unknown auto_scaling_strategy.on_hydration key '" ~ key ~ "'. "
                    ~ "Supported keys: hydration_size, linger_duration."
                ) }}
            {% endif %}
        {% endfor %}
        {% if size is not none and on_hydration.get('hydration_size') == size %}
            {{ exceptions.raise_compiler_error(
                "auto_scaling_strategy.on_hydration.hydration_size must differ "
                ~ "from the cluster size '" ~ size ~ "'. A burst to the same "
                ~ "size is a no-op."
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{#
Renders an `auto_scaling_strategy` mapping as the corresponding
`AUTO SCALING STRATEGY = (...)` clause. An empty mapping renders the
disabling form `AUTO SCALING STRATEGY = ()`.
#}
{% macro internal_render_auto_scaling_strategy(auto_scaling_strategy) %}
    {% if not auto_scaling_strategy %}
        {{ return("AUTO SCALING STRATEGY = ()") }}
    {% endif %}
    {% set on_hydration = auto_scaling_strategy.get('on_hydration') %}
    {% set parts = ["HYDRATION SIZE = " ~ dbt.string_literal(on_hydration.get('hydration_size'))] %}
    {% if on_hydration.get('linger_duration') is not none %}
        {% do parts.append("LINGER DURATION = " ~ dbt.string_literal(on_hydration.get('linger_duration'))) %}
    {% endif %}
    {{ return("AUTO SCALING STRATEGY = (ON HYDRATION (" ~ parts | join(", ") ~ "))") }}
{% endmacro %}

{#
Reads the configured autoscaling strategy of a cluster from
mz_internal.mz_cluster_auto_scaling_strategies and returns it as an
`auto_scaling_strategy` mapping, or none when the cluster has no strategy
configured (or the server predates the feature).

NOTE: the catalog stores the linger duration as a {secs, nanos} object. The
read-back keeps whole seconds only, so sub-second linger components are
dropped when the strategy is copied to another cluster.
#}
{% macro internal_get_cluster_auto_scaling_strategy(cluster_name) %}
    {% set catalog_relation_exists %}
        SELECT count(*) > 0
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        WHERE s.name = 'mz_internal'
            AND o.name = 'mz_cluster_auto_scaling_strategies'
    {% endset %}

    {% set exists_result = run_query(catalog_relation_exists) %}
    {% if not execute or not exists_result.rows[0][0] %}
        {{ return(none) }}
    {% endif %}

    {% set strategy_query %}
        SELECT
            s.strategy->'on_hydration'->>'hydration_size' AS hydration_size,
            (s.strategy->'on_hydration'->'linger_duration'->>'secs')::bigint AS linger_secs
        FROM mz_internal.mz_cluster_auto_scaling_strategies s
        JOIN mz_clusters c ON c.id = s.cluster_id
        WHERE c.name = {{ dbt.string_literal(cluster_name) }}
    {% endset %}

    {% set strategy_result = run_query(strategy_query) %}
    {# A row can exist with a null strategy while a burst from a just-removed
       policy is still draining, so also require a hydration size. #}
    {% if strategy_result.rows and strategy_result.rows[0][0] is not none %}
        {% set on_hydration = {'hydration_size': strategy_result.rows[0][0]} %}
        {% if strategy_result.rows[0][1] is not none %}
            {% do on_hydration.update({'linger_duration': strategy_result.rows[0][1] ~ 's'}) %}
        {% endif %}
        {{ return({'on_hydration': on_hydration}) }}
    {% endif %}

    {{ return(none) }}
{% endmacro %}
