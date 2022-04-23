# Copyright 2020 Josh Wills. All rights reserved.
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

from dataclasses import dataclass
from typing import Optional, List, Any

from dbt.adapters.base.meta import available
from dbt.adapters.materialize import MaterializeConnectionManager
from dbt.adapters.materialize.relation import MaterializeRelation
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.postgres.impl import PostgresIndexConfig

@dataclass
class MaterializeIndexConfig(PostgresIndexConfig):
     columns: List[str]
     type: Optional[str] = None
     name: Optional[str] = None

     @classmethod
     def parse(cls, raw_index) -> Optional["MaterializeIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.raise_compiler_error(f"Could not parse index config: {msg}")
        except TypeError:
            dbt.exceptions.raise_compiler_error(
                f"Invalid index config:\n"
                f"  Got: {raw_index}\n"
                f'  Expected a dictionary with at minimum a "columns" key'
            )


class MaterializeAdapter(PostgresAdapter):
    ConnectionManager = MaterializeConnectionManager
    Relation = MaterializeRelation

    def _link_cached_relations(self, manifest):
        # NOTE(benesch): this *should* reimplement the parent class's method
        # for Materialize, but presently none of our tests care if we just
        # disable this method instead.
        #
        # [0]: https://github.com/dbt-labs/dbt-core/blob/13b18654f03d92eab3f5a9113e526a2a844f145d/plugins/postgres/dbt/adapters/postgres/impl.py#L126-L133
        pass

    @available
    def parse_index(self, raw_index: Any) -> Optional[MaterializeIndexConfig]:
        return MaterializeIndexConfig.parse(raw_index)
