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

# The types of the binding in `src/lib.rs`, which documents them.
# `mypy.stubtest` checks them against the built module, so a change to either
# fails until both match.

import typing

__all__ = ["Subscription", "conformance"]

@typing.final
class Subscription:
    def __new__(cls, options: str) -> Subscription: ...
    def declare_sql(self) -> str: ...
    def fetch(self, input: str) -> str: ...
    def push(self, input: str) -> str: ...
    def classify(self, input: str) -> str: ...
    @property
    def is_finished(self) -> bool: ...
    @property
    def is_failed(self) -> bool: ...
    @property
    def as_of(self) -> int | None: ...

def conformance(function: str, input: str) -> str: ...
