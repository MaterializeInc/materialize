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
from typing import Any


def write_results_to_file(results: list[Any], output_file_path: str) -> None:
    with open(output_file_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        print(f"Written data to {output_file_path}")


def read_results_from_file(file_path: str) -> list[Any]:
    with open(file_path) as f:
        data = json.load(f)
        print(f"Loaded data from {file_path}")
        return data
