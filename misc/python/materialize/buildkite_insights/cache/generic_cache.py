# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from materialize import MZ_ROOT
from materialize.buildkite_insights.cache.cache_constants import (
    FetchMode,
)
from materialize.buildkite_insights.util.data_io import (
    FilePath,
    exists_file,
    exists_file_with_recent_data,
    get_last_modification_date,
    read_results_from_file,
    write_results_to_file,
)
from materialize.util import ensure_dir_exists

PATH_TO_CACHE_DIR = MZ_ROOT / "temp"


@dataclass
class CacheFilePath(FilePath):
    cache_item_type: str
    pipeline_slug: str
    params_hash: str
    file_extension: str = "json"

    def get_path_to_directory(self):
        return f"{PATH_TO_CACHE_DIR}/{self.cache_item_type}"

    def get(self) -> str:
        return f"{self.get_path_to_directory()}/{self.pipeline_slug}-params-{self.params_hash}.{self.file_extension}"


def get_or_query_data(
    cache_file_path: CacheFilePath,
    fetch_action: Callable[[], Any],
    fetch_mode: FetchMode,
    max_allowed_cache_age_in_hours: int | None = 12,
    add_to_cache_if_not_present: bool = True,
    quiet_mode: bool = False,
) -> Any:
    ensure_dir_exists(cache_file_path.get_path_to_directory())

    no_fetch = fetch_mode == FetchMode.NEVER

    if no_fetch and not exists_file(cache_file_path):
        raise RuntimeError(f"File missing: {cache_file_path}")

    if fetch_mode == FetchMode.AVOID and exists_file(cache_file_path):
        no_fetch = True
    elif fetch_mode == FetchMode.AUTO and exists_file_with_recent_data(
        cache_file_path, max_allowed_cache_age_in_hours
    ):
        no_fetch = True

    if no_fetch:
        if not quiet_mode:
            last_modified_date = get_last_modification_date(cache_file_path)
            print(
                f"Using existing data: {cache_file_path} (downloaded: {last_modified_date})"
            )
        return read_results_from_file(cache_file_path, quiet_mode=quiet_mode)

    fetched_data = fetch_action()

    if add_to_cache_if_not_present:
        write_results_to_file(fetched_data, cache_file_path, quiet_mode=quiet_mode)

    return fetched_data
