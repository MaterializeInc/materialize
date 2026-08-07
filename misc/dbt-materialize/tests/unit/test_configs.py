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
from dbt.adapters.materialize.exceptions import (
    PartitionByConfigError,
    RefreshIntervalConfigError,
)
from dbt.adapters.materialize.impl import (
    MaterializeAdapter,
    MaterializeIndexConfig,
    MaterializeRefreshIntervalConfig,
)


class TestIndexConfig:
    def test_none(self):
        assert MaterializeIndexConfig.parse(None) is None

    def test_columns(self):
        index = MaterializeIndexConfig.parse({"columns": ["a", "b"]})

        assert index.columns == ["a", "b"]
        assert index.default is False
        assert index.name is None
        assert index.cluster is None

    def test_all_fields(self):
        index = MaterializeIndexConfig.parse(
            {"columns": ["a"], "default": True, "name": "my_idx", "cluster": "my_cluster"}
        )

        assert index.default is True
        assert index.name == "my_idx"
        assert index.cluster == "my_cluster"


class TestRefreshIntervalConfig:
    def test_none(self):
        assert MaterializeRefreshIntervalConfig.parse(None) is None

    def test_every(self):
        refresh_interval = MaterializeRefreshIntervalConfig.parse(
            {"every": "1 day", "aligned_to": "2024-01-01"}
        )

        assert refresh_interval.every == "1 day"
        assert refresh_interval.aligned_to == "2024-01-01"
        assert refresh_interval.on_commit is False

    def test_invalid_field_type(self):
        with pytest.raises(RefreshIntervalConfigError):
            MaterializeRefreshIntervalConfig.parse({"every": 1})


class TestPartitionByConfig:
    @pytest.fixture
    def adapter(self):
        # parse_partition_by only validates the shape of the config, so it does
        # not need a connected adapter.
        return MaterializeAdapter.__new__(MaterializeAdapter)

    def test_none(self, adapter):
        assert adapter.parse_partition_by(None) is None

    def test_empty_list(self, adapter):
        assert adapter.parse_partition_by([]) is None

    def test_columns(self, adapter):
        assert adapter.parse_partition_by(["a", "b"]) == ["a", "b"]

    @pytest.mark.parametrize("raw", ["a", {"columns": ["a"]}, ["a", 1]])
    def test_invalid(self, adapter, raw):
        with pytest.raises(PartitionByConfigError):
            adapter.parse_partition_by(raw)
