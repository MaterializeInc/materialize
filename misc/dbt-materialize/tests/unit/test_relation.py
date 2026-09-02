# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from dbt.adapters.materialize.relation import MaterializeRelation


def make_relation(relation_type):
    return MaterializeRelation.create(
        database="materialize",
        schema="public",
        identifier="my_relation",
        type=relation_type,
    )


@pytest.mark.parametrize(
    "relation_type,expected",
    [
        ("materialized_view", True),
        # The legacy materialization name is still supported.
        ("materializedview", True),
        ("view", False),
        ("table", False),
    ],
)
def test_is_materialized_view(relation_type, expected):
    assert make_relation(relation_type).is_materialized_view is expected


@pytest.mark.parametrize(
    "relation_type,attribute",
    [
        ("source", "is_source"),
        ("source_table", "is_source_table"),
        ("sink", "is_sink"),
    ],
)
def test_materialize_specific_types(relation_type, attribute):
    relation = make_relation(relation_type)

    assert getattr(relation, attribute) is True
    assert relation.is_view is False


def test_relation_max_name_length():
    # Materialize does not have PostgreSQL's 63 character limit.
    assert make_relation("view").relation_max_name_length() == 255
