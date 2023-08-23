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
from typing import Any, Generator, Optional

import requests

from materialize.cloudtest.util.authentication import AuthConfig
from materialize.cloudtest.util.common import eprint


@contextmanager
def verbose_http_errors() -> Generator[None, None, None]:
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
    auth: Optional[AuthConfig],
    base_url: str,
    path: str,
    timeout: int = 15,
) -> requests.Response:
    eprint(f"GET {base_url}{path}")

    def try_get() -> requests.Response:
        with verbose_http_errors():
            headers = _create_headers(auth)
            response = requests.get(
                f"{base_url}{path}",
                headers=headers,
                timeout=timeout,
                cert=_get_cert(auth),
            )
            response.raise_for_status()
            return response

    try:
        response = try_get()
    except requests.exceptions.HTTPError as e:
        if auth and e.response.status_code == 401:
            auth.refresh()
            response = try_get()
        else:
            raise

    return response


def post(
    auth: Optional[AuthConfig],
    base_url: str,
    path: str,
    json: Any,
    timeout: int = 15,
) -> requests.Response:
    eprint(f"POST {base_url}{path}")

    def try_post() -> requests.Response:
        with verbose_http_errors():
            headers = _create_headers(auth)
            response = requests.post(
                f"{base_url}{path}",
                headers=headers,
                json=json,
                timeout=timeout,
                cert=_get_cert(auth),
            )
            response.raise_for_status()
            return response

    try:
        response = try_post()
    except requests.exceptions.HTTPError as e:
        if auth and e.response.status_code == 401:
            auth.refresh()
            response = try_post()
        else:
            raise

    return response


def patch(
    auth: Optional[AuthConfig],
    base_url: str,
    path: str,
    json: Any,
    timeout: int = 15,
) -> requests.Response:
    eprint(f"PATCH {base_url}{path}")

    def try_patch() -> requests.Response:
        with verbose_http_errors():
            headers = _create_headers(auth)
            response = requests.patch(
                f"{base_url}{path}",
                headers=headers,
                json=json,
                timeout=timeout,
                cert=_get_cert(auth),
            )
            response.raise_for_status()
            return response

    try:
        response = try_patch()
    except requests.exceptions.HTTPError as e:
        if auth and e.response.status_code == 401:
            auth.refresh()
            response = try_patch()
        else:
            raise

    return response


def delete(
    auth: Optional[AuthConfig],
    base_url: str,
    path: str,
    params: Any = None,
    timeout: int = 15,
) -> requests.Response:
    eprint(f"DELETE {base_url}{path}")

    def try_delete() -> requests.Response:
        with verbose_http_errors():
            headers = _create_headers(auth)
            response = requests.delete(
                f"{base_url}{path}",
                headers=headers,
                timeout=timeout,
                cert=_get_cert(auth),
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
        if auth and e.response.status_code == 401:
            auth.refresh()
            response = try_delete()
        else:
            raise

    return response


def _create_headers(auth: Optional[AuthConfig]) -> dict[str, Any]:
    headers = {}
    if auth:
        headers["Authorization"] = f"Bearer {auth.token}"

    return headers


def _get_cert(auth: Optional[AuthConfig]) -> Optional[tuple[str, str]]:
    return auth.client_cert if auth is not None else None
