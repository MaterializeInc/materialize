# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest
from pg8000.exceptions import DatabaseError, InterfaceError

from materialize.mzbisect.db import error_message, ident, literal


def test_ident_always_quotes() -> None:
    assert ident("foo") == '"foo"'


def test_ident_escapes_embedded_quotes() -> None:
    assert ident('weird"name') == '"weird""name"'


def test_ident_rejects_nul() -> None:
    with pytest.raises(AssertionError):
        ident("foo\0bar")


def test_literal_escapes_embedded_quotes() -> None:
    assert literal("O'Brien") == "'O''Brien'"


def test_literal_rejects_nul() -> None:
    with pytest.raises(AssertionError):
        literal("foo\0bar")


def test_error_message_extracts_server_fields() -> None:
    e = DatabaseError({"M": "Non-positive multiplicity", "D": "in DistinctBy"})
    assert error_message(e) == "Non-positive multiplicity (in DistinctBy)"


def test_error_message_without_detail() -> None:
    e = DatabaseError({"M": "boom"})
    assert error_message(e) == "boom"


def test_error_message_falls_back_to_str() -> None:
    assert error_message(InterfaceError("network went away")) == "network went away"
