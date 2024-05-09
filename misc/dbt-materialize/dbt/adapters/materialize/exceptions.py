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

from typing import Any

from dbt_common.exceptions import CompilationError


class RefreshIntervalConfigNotDictError(CompilationError):
    def __init__(self, raw_refresh_interval: Any):
        self.raw_refresh_interval = raw_refresh_interval
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f"Invalid refresh_interval config:\n"
            f"  Got: {self.raw_refresh_interval}\n"
            f'  Expected a dictionary with at minimum a "at", "at_creation", or "every" key'
        )
        return msg


class RefreshIntervalConfigError(CompilationError):
    def __init__(self, exc: TypeError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse refresh interval config: {validator_msg}"
        return msg
