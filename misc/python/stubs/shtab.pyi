# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from argparse import ArgumentParser

def add_argument_to(
    parser: ArgumentParser,
    option_string: str | list[str] = ...,
    help: str = ...,
    parent: ArgumentParser | None = ...,
    preamble: str = ...,
) -> None: ...
