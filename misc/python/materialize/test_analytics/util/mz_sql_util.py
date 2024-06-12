# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


def sanitize_text_value(value: str) -> str:
    return value.replace("'", "\\'")


def as_sanitized_literal(value: str | None, sanitize_value: bool = True) -> str:
    if value is None:
        return "NULL"

    if sanitize_value:
        value = sanitize_text_value(value)

    return f"E'{value}'"
