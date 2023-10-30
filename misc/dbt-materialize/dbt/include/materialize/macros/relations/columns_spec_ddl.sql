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

# NOTE(morsapaes): if we don't override the macro, a warning is thrown but dbt
# still tries to parse the constraint, which leads to a confusing error for
# users.
{% macro materialize__get_table_columns_and_constraints() -%}
  {{ exceptions.warn(
        """
        Materialize does not support constraints.
        """
    )}}
{%- endmacro %}
