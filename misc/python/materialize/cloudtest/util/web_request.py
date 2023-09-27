# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

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


class WebRequests:
    def __init__(
        self,
        auth: AuthConfig | None,
        base_url: str,
        client_cert: tuple[str, str] | None = None,
        additional_headers: dict[str, str] | None = None,
        default_timeout_in_sec: int = 15,
    ):
        self.auth = auth
        self.base_url = base_url
        self.client_cert = client_cert
        self.additional_headers = additional_headers
        self.default_timeout_in_sec = default_timeout_in_sec

    def get(
        self,
        path: str,
        timeout_in_sec: int | None = None,
    ) -> requests.Response:
        eprint(f"GET {self.base_url}{path}")

        def try_get() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = requests.get(
                    f"{self.base_url}{path}",
                    headers=headers,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                )
                response.raise_for_status()
                return response

        try:
            response = try_get()
        except requests.exceptions.HTTPError as e:
            if self.auth and e.response.status_code == 401:
                self.auth.refresh()
                response = try_get()
            else:
                raise

        return response

    def post(
        self,
        path: str,
        json: Any,
        timeout_in_sec: int | None = None,
    ) -> requests.Response:
        eprint(f"POST {self.base_url}{path}")

        def try_post() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = requests.post(
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                )
                response.raise_for_status()
                return response

        try:
            response = try_post()
        except requests.exceptions.HTTPError as e:
            if self.auth and e.response.status_code == 401:
                self.auth.refresh()
                response = try_post()
            else:
                raise

        return response

    def patch(
        self,
        path: str,
        json: Any,
        timeout_in_sec: int | None = None,
    ) -> requests.Response:
        eprint(f"PATCH {self.base_url}{path}")

        def try_patch() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = requests.patch(
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                )
                response.raise_for_status()
                return response

        try:
            response = try_patch()
        except requests.exceptions.HTTPError as e:
            if self.auth and e.response.status_code == 401:
                self.auth.refresh()
                response = try_patch()
            else:
                raise

        return response

    def delete(
        self,
        path: str,
        params: Any = None,
        timeout_in_sec: int | None = None,
    ) -> requests.Response:
        eprint(f"DELETE {self.base_url}{path}")

        def try_delete() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = requests.delete(
                    f"{self.base_url}{path}",
                    headers=headers,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
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
            if self.auth and e.response.status_code == 401:
                self.auth.refresh()
                response = try_delete()
            else:
                raise

        return response

    def _create_headers(self, auth: AuthConfig | None) -> dict[str, Any]:
        headers = self.additional_headers.copy() if self.additional_headers else {}
        if auth:
            headers["Authorization"] = f"Bearer {auth.token}"

        return headers

    def _timeout_or_default(self, timeout_in_sec: int | None) -> int:
        return timeout_in_sec or self.default_timeout_in_sec
