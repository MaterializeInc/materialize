#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

from materialize import MZ_ROOT

PATH_TO_TEMP_DIR = MZ_ROOT / "temp"


def get_file_path(
    file_prefix: str,
    pipeline_slug: str,
    params_hash: str,
) -> str:
    return f"{PATH_TO_TEMP_DIR}/{file_prefix}-{pipeline_slug}-params-{params_hash}.json"


def ensure_temp_dir_exists() -> None:
    subprocess.run(
        [
            "mkdir",
            "-p",
            f"{PATH_TO_TEMP_DIR}",
        ],
        check=True,
    )


def write_results_to_file(
    results: list[Any], output_file_path: str, quiet_mode: bool = False
) -> None:
    with open(output_file_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        if not quiet_mode:
            print(f"Written data to {output_file_path}")


def read_results_from_file(file_path: str, quiet_mode: bool = False) -> list[Any]:
    with open(file_path) as f:
        data = json.load(f)
        if not quiet_mode:
            print(f"Loaded data from {file_path}")
        return data


def exists_file_with_recent_data(
    file_path: str, max_allowed_cache_age_in_hours: int | None
) -> bool:
    if not os.path.isfile(file_path):
        return False

    if max_allowed_cache_age_in_hours is None:
        return True

    modification_date_as_sec_since_epoch = os.path.getmtime(file_path)
    modification_date = datetime.utcfromtimestamp(modification_date_as_sec_since_epoch)

    max_modification_date = datetime.now() - timedelta(
        hours=max_allowed_cache_age_in_hours
    )

    return modification_date > max_modification_date
