# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

def split(sql: str) -> List[str]: ...
def format(
    sql: str,
    encoding: Optional[str] = ...,
    *,
    keyword_case: str = ...,
    identifier_case: str = ...,
    strip_comments: bool = ...,
    truncate_strings: int = ...,
    truncate_char: str = ...,
    reindent: bool = ...,
    reindent_aligned: bool = ...,
    use_space_around_operators: bool = ...,
    indent_tabs: bool = ...,
    indent_width: int = ...,
    wrap_after: int = ...,
    output_format: str = ...,
    comma_first: bool = ...,
) -> str: ...
