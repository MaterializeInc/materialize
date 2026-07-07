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

import base64
import json

import pytest

from materialize_subscribe import InvalidToken, ResumeToken


def test_as_of_is_frontier_minus_one_and_saturates() -> None:
    assert ResumeToken(frontier=0, fingerprint="q").as_of() == 0
    assert ResumeToken(frontier=1, fingerprint="q").as_of() == 0
    assert ResumeToken(frontier=5, fingerprint="q").as_of() == 4


def test_encode_decode_round_trips() -> None:
    token = ResumeToken(frontier=42, fingerprint="fingerprint-abc")
    decoded = ResumeToken.decode(token.encode())
    assert decoded == token
    assert decoded.frontier == 42
    assert decoded.fingerprint == "fingerprint-abc"


def test_encoding_is_stable_and_cross_language() -> None:
    # This exact string is also asserted by the Rust SDK's test suite, so a
    # token minted by either SDK decodes in the other. If this changes, the two
    # SDKs have diverged and tokens are no longer interchangeable.
    encoded = ResumeToken(frontier=42, fingerprint="x").encode()
    assert encoded == "eyJmb3JtYXQiOjEsImZyb250aWVyIjo0MiwiZmluZ2VycHJpbnQiOiJ4In0"


def test_decode_rejects_garbage() -> None:
    with pytest.raises(InvalidToken):
        ResumeToken.decode("!!! not base64 !!!")
    with pytest.raises(InvalidToken):
        ResumeToken.decode(base64.urlsafe_b64encode(b"not json").decode())


def test_decode_rejects_unknown_format() -> None:
    future = json.dumps({"format": 999, "frontier": 1, "fingerprint": "q"}).encode()
    encoded = base64.urlsafe_b64encode(future).decode().rstrip("=")
    with pytest.raises(InvalidToken):
        ResumeToken.decode(encoded)
