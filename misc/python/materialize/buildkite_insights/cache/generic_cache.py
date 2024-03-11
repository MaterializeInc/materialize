#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable
from typing import Any

from materialize.buildkite_insights.cache.cache_constants import (
    FETCH_MODE_AUTO,
    FETCH_MODE_NO,
)
from materialize.buildkite_insights.util.data_io import (
    ensure_temp_dir_exists,
    exists_file_with_recent_data,
    read_results_from_file,
    write_results_to_file,
)


def get_or_query_data(
    cache_file_path: str,
    fetch_action: Callable[[], list[Any]],
    fetch_mode: str,
    max_allowed_cache_age_in_hours: int | None = 10,
    add_to_cache_if_not_present: bool = True,
    quiet_mode: bool = False,
) -> list[Any]:
    ensure_temp_dir_exists()

    no_fetch = fetch_mode == FETCH_MODE_NO

    if fetch_mode == FETCH_MODE_AUTO and exists_file_with_recent_data(
        cache_file_path, max_allowed_cache_age_in_hours
    ):
        no_fetch = True

    if no_fetch:
        if not quiet_mode:
            print(f"Using existing data: {cache_file_path}")
        return read_results_from_file(cache_file_path, quiet_mode=quiet_mode)

    fetched_data = fetch_action()

    if add_to_cache_if_not_present:
        write_results_to_file(fetched_data, cache_file_path, quiet_mode=quiet_mode)

    return fetched_data
