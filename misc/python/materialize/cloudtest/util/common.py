# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import subprocess
from collections.abc import Callable
from textwrap import dedent
from time import sleep
from typing import Any, cast

from materialize.mzcompose.composition import filter_cmd

LOGGER = logging.getLogger(__name__)


def retry(
    f: Callable[[], Any],
    max_attempts: int,
    exception_types: list[type[Exception]],
    sleep_secs: int = 1,
    message: str | None = None,
) -> Any:
    result: Any = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = f()
            break
        except tuple(exception_types) as e:
            if attempt == max_attempts:
                if message:
                    LOGGER.info(message)
                else:
                    LOGGER.error(f"Exception in attempt {attempt}: ", exc_info=e)
                raise
            sleep(sleep_secs)
    return result


def is_subdict(
    larger_dict: dict[str, Any],
    smaller_dict: dict[str, Any],
    key_path: str = "",
) -> bool:
    def is_sublist(
        larger_list: list[Any], smaller_list: list[Any], key_path: str = ""
    ) -> bool:
        # All members of list must exist in larger_dict's list,
        # but if they are dicts, they are allowed to be subdicts,
        # rather than exact matches.
        if len(larger_list) < len(smaller_list):
            LOGGER.warning(f"{key_path}: smaller_list is larger than larger_list")
            return False
        for i, value in enumerate(smaller_list):
            current_key = f"{key_path}.{i}"
            if isinstance(value, dict):
                if not is_subdict(
                    larger_list[i],
                    cast(dict[str, Any], value),
                    current_key,
                ):
                    return False
            elif isinstance(value, list):
                if not is_sublist(
                    larger_list[i],
                    value,
                    current_key,
                ):
                    return False
            else:
                if value != larger_list[i]:
                    LOGGER.warning(
                        f"{key_path}.{i}: scalar value does not match: {value} != {larger_list[i]}",
                    )
                    return False
        return True

    for key, value in smaller_dict.items():
        current_key = f"{key_path}.{key}"
        if key not in larger_dict:
            LOGGER.warning(f"{key_path}.{key}: key not found in larger_dict")
            return False
        if isinstance(value, dict):
            if not is_subdict(
                larger_dict[key],
                cast(dict[str, Any], value),
                current_key,
            ):
                return False
        elif isinstance(value, list):
            if not is_sublist(
                larger_dict[key],
                value,
                current_key,
            ):
                return False
        else:
            if value != larger_dict[key]:
                LOGGER.warning(
                    f"{current_key}: scalar value does not match: {value} != {larger_dict[key]}",
                )
                return False
    return True


def run_process_with_error_information(
    cmd: list[str], input: str | None = None, capture_output: bool = False
) -> None:
    try:
        subprocess.run(
            cmd, text=True, input=input, check=True, capture_output=capture_output
        )
    except subprocess.CalledProcessError as e:
        log_subprocess_error(e)
        raise e


def log_subprocess_error(e: subprocess.CalledProcessError) -> None:
    LOGGER.error(
        dedent(
            f"""
                cmd: {filter_cmd(e.cmd)}
                returncode: {e.returncode}
                stdout: {e.stdout}
                stderr: {e.stderr}
                """
        )
    )
