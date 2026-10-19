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

"""The SDK's exceptions, which the binding raises."""

from __future__ import annotations

from typing import Any


class SdkError(Exception):
    """An error of the Materialize SDK."""


class ProtocolError(SdkError):
    """The server broke the subscribe protocol, and the subscription failed."""


class TimestampTooLarge(SdkError):
    """One timestamp's updates exceeded the memory limit, and the subscription failed."""


class UsageError(SdkError):
    """The SDK was called in a way its contract does not allow."""


class ServerError(SdkError):
    """Materialize returned an error, or the connection failed.

    ``classification`` is the protocol core's verdict, such as
    ``{"action": "kind", "kind": "transient"}``.
    """

    def __init__(
        self, message: str, sqlstate: str | None, classification: dict[str, Any]
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.classification = classification
