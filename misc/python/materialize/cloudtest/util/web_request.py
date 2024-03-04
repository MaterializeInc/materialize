# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
from collections.abc import Generator
from contextlib import contextmanager
from ipaddress import IPv4Address, IPv6Address
from textwrap import dedent
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import DEFAULT_POOLBLOCK, HTTPAdapter, Retry

from materialize.cloudtest.util.authentication import AuthConfig

LOGGER = logging.getLogger(__name__)


@contextmanager
def verbose_http_errors() -> Generator[None, None, None]:
    try:
        yield
    except requests.HTTPError as e:
        LOGGER.error(
            dedent(
                f"""
                response status: {e.response.status_code}
                response reason: {e.response.reason}
                response content: {e.response.content}
                """
            )
        )
        raise


class DNSResolverHTTPSAdapter(HTTPAdapter):
    def __init__(self, common_name, ip, **kwargs):
        self.__common_name = common_name
        self.__ip = str(ip)
        super().__init__(**kwargs)

    def get_connection(self, url, proxies=None):
        redirected_url = url.replace(self.__common_name.lower(), self.__ip)
        LOGGER.info(f"original url: {url}")
        LOGGER.info(f"redirected url: {redirected_url}")
        return super().get_connection(
            redirected_url,
            proxies=proxies,
        )

    def init_poolmanager(
        self,
        connections,
        maxsize,
        block=DEFAULT_POOLBLOCK,
        **pool_kwargs,
    ):
        pool_kwargs["assert_hostname"] = self.__common_name
        pool_kwargs["server_hostname"] = self.__common_name
        super().init_poolmanager(
            connections,
            maxsize,
            block,
            **pool_kwargs,
        )


class WebRequests:
    def __init__(
        self,
        auth: AuthConfig | None,
        base_url: str,
        client_cert: tuple[str, str] | None = None,
        additional_headers: dict[str, str] | None = None,
        default_timeout_in_sec: int = 15,
        override_ip: IPv4Address | IPv6Address | None = None,
        verify: str | None = None,
    ):
        self.auth = auth
        self.base_url = base_url
        self.client_cert = client_cert
        self.additional_headers = additional_headers
        self.default_timeout_in_sec = default_timeout_in_sec
        self.override_ip = override_ip
        self.verify = verify

    def session(self) -> requests.Session:
        session = requests.Session()
        if self.override_ip is not None:
            parsed_url = urlparse(self.base_url)
            session.mount(
                self.base_url.lower(),
                DNSResolverHTTPSAdapter(
                    parsed_url.netloc.split(":", 1)[0],
                    self.override_ip,
                ),
            )
        return session

    def get(
        self,
        path: str,
        timeout_in_sec: int | None = None,
    ) -> requests.Response:
        LOGGER.info(f"GET {self.base_url}{path}")

        def try_get() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                s = self.session()
                s.mount(self.base_url, HTTPAdapter(max_retries=Retry(3)))
                response = s.get(
                    f"{self.base_url}{path}",
                    headers=headers,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                    verify=self.verify,
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
        LOGGER.info(f"POST {self.base_url}{path}")

        def try_post() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = self.session().post(
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                    verify=self.verify,
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
        LOGGER.info(f"PATCH {self.base_url}{path}")

        def try_patch() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = self.session().patch(
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                    verify=self.verify,
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
        LOGGER.info(f"DELETE {self.base_url}{path}")

        def try_delete() -> requests.Response:
            with verbose_http_errors():
                headers = self._create_headers(self.auth)
                response = self.session().delete(
                    f"{self.base_url}{path}",
                    headers=headers,
                    timeout=self._timeout_or_default(timeout_in_sec),
                    cert=self.client_cert,
                    verify=self.verify,
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
