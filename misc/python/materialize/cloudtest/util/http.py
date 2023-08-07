# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from contextlib import contextmanager
from typing import Any

import requests

from materialize.cloudtest.config.environment_config import EnvironmentConfig
from materialize.cloudtest.util.common import eprint


@contextmanager
def verbose_http_errors():
    try:
        yield
    except requests.HTTPError as e:
        print(
            e.response.status_code,
            e.response.reason,
            e.response.content,
            file=sys.stderr,
        )
        raise


def get(
    config: EnvironmentConfig,
    base_url: str,
    path: str,
    timeout: int = 15,
    use_token: bool = True,
):
    eprint(f"GET {base_url}{path}")

    def try_get():
        with verbose_http_errors():
            headers = {}
            if use_token:
                headers["Authorization"] = f"Bearer {config.auth.token}"
            response = requests.get(
                f"{base_url}{path}",
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response

    try:
        response = try_get()
    except requests.exceptions.HTTPError as e:
        if use_token and e.response.status_code == 401:
            config.refresh_auth()
            response = try_get()
        else:
            raise

    return response


def post(
    config: EnvironmentConfig,
    base_url: str,
    path: str,
    json: Any,
    timeout: int = 15,
    use_token: bool = True,
):
    eprint(f"POST {base_url}{path}")

    def try_post():
        with verbose_http_errors():
            headers = {}
            if use_token:
                headers["Authorization"] = f"Bearer {config.auth.token}"
            response = requests.post(
                f"{base_url}{path}",
                headers=headers,
                json=json,
                timeout=timeout,
            )
            response.raise_for_status()
            return response

    try:
        response = try_post()
    except requests.exceptions.HTTPError as e:
        if use_token and e.response.status_code == 401:
            config.refresh_auth()
            response = try_post()
        else:
            raise

    return response


def patch(
    config: EnvironmentConfig,
    base_url: str,
    path: str,
    json: Any,
    timeout: int = 15,
    use_token: bool = True,
):
    eprint(f"PATCH {base_url}{path}")

    def try_patch():
        with verbose_http_errors():
            headers = {}
            if use_token:
                headers["Authorization"] = f"Bearer {config.auth.token}"
            response = requests.patch(
                f"{base_url}{path}",
                headers=headers,
                json=json,
                timeout=timeout,
            )
            response.raise_for_status()
            return response

    try:
        response = try_patch()
    except requests.exceptions.HTTPError as e:
        if use_token and e.response.status_code == 401:
            config.refresh_auth()
            response = try_patch()
        else:
            raise

    return response


def delete(
    config: EnvironmentConfig,
    base_url: str,
    path: str,
    params: Any = None,
    timeout: int = 15,
    use_token: bool = True,
):
    eprint(f"DELETE {base_url}{path}")

    def try_delete():
        with verbose_http_errors():
            headers = {}
            if use_token:
                headers["Authorization"] = f"Bearer {config.auth.token}"
            response = requests.delete(
                f"{base_url}{path}",
                headers=headers,
                timeout=timeout,
                **(
                    {
                        "params": params,
                    }
                    if params is not None
                    else {}
                ),
            )
            response.raise_for_status()
            return response

    try:
        response = try_delete()
    except requests.exceptions.HTTPError as e:
        if use_token and e.response.status_code == 401:
            config.refresh_auth()
            response = try_delete()
        else:
            raise

    return response
