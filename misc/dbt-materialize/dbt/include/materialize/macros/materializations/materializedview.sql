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

{% materialization materializedview, adapter='materialize' %}
  {{ exceptions.warn(
        """
        The `materializedview` materialization name is deprecated and will be
        removed in a future release of dbt-materialize. Please use
        `materialized_view` instead, which is built-in from dbt v1.6:

        {{ config( materialized = 'materialized_view' )}}
        """
    )}}
  {{ return(materialization_materialized_view_materialize()) }}
{% endmaterialization %}
