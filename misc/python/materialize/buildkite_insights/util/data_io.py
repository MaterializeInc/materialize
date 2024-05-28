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
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass
class FilePath:
    def get(self) -> str:
        raise NotImplementedError

    def __str__(self):
        return self.get()


@dataclass
class SimpleFilePath(FilePath):
    file_name: str

    def get(self) -> str:
        return self.file_name


def write_results_to_file(
    results: list[Any], output_file_path: FilePath, quiet_mode: bool = False
) -> None:
    with open(output_file_path.get(), "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        if not quiet_mode:
            print(f"Written data to {output_file_path}")


def read_results_from_file(file_path: FilePath, quiet_mode: bool = False) -> list[Any]:
    with open(file_path.get()) as f:
        data = json.load(f)
        if not quiet_mode:
            print(f"Loaded data from {file_path}")
        return data


def exists_file_with_recent_data(
    file_path: FilePath, max_allowed_cache_age_in_hours: int | None
) -> bool:
    if not os.path.isfile(file_path.get()):
        return False

    if max_allowed_cache_age_in_hours is None:
        return True

    modification_date_as_sec_since_epoch = os.path.getmtime(file_path.get())
    modification_date = datetime.utcfromtimestamp(modification_date_as_sec_since_epoch)

    max_modification_date = datetime.now() - timedelta(
        hours=max_allowed_cache_age_in_hours
    )

    return modification_date > max_modification_date
