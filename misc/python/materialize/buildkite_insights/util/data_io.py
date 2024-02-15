#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

from materialize import MZ_ROOT

PATH_TO_TEMP_DIR = MZ_ROOT / "temp"


def get_file_path(
    pipeline_slug: str,
    branch: str | None,
    build_state: str | None,
    max_fetches: int,
    items_per_page: int,
) -> str:
    max_entries = max_fetches * items_per_page
    meta_data = f"{branch}-{build_state}-{max_entries}"
    hash_value = hashlib.sha256(bytes(meta_data, encoding="utf-8")).hexdigest()[:8]
    return f"{PATH_TO_TEMP_DIR}/builds-{pipeline_slug}-params-{hash_value}.json"


def ensure_temp_dir_exists() -> None:
    subprocess.run(
        [
            "mkdir",
            "-p",
            f"{PATH_TO_TEMP_DIR}",
        ],
        check=True,
    )


def write_results_to_file(results: list[Any], output_file_path: str) -> None:
    with open(output_file_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        print(f"Written data to {output_file_path}")


def read_results_from_file(file_path: str) -> list[Any]:
    with open(file_path) as f:
        data = json.load(f)
        print(f"Loaded data from {file_path}")
        return data


def exists_file_with_recent_data(file_path: str) -> bool:
    if not os.path.isfile(file_path):
        return False

    modification_date_as_sec_since_epoch = os.path.getmtime(file_path)
    modification_date = datetime.utcfromtimestamp(modification_date_as_sec_since_epoch)

    max_modification_date = datetime.now() - timedelta(hours=8)

    return modification_date > max_modification_date
