# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, fields

import requests
from requests.exceptions import ConnectionError, ReadTimeout

from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.jwt_key import fetch_jwt


@dataclass
class AuthConfig:
    organization_id: str
    token: str
    app_user: str | None
    app_password: str | None

    refresh_fn: Callable[[AuthConfig], None]

    pgwire_ssl_mode: str = "require"
    tls_ca_cert_path: str | None = None

    def refresh(self) -> None:
        self.refresh_fn(self)


@dataclass
class TestUserConfig:
    email: str
    password: str
    frontegg_host: str


DEFAULT_ORG_ID = "80b1a04a-2277-11ed-a1ce-5405dbb9e0f7"


# TODO: this retry loop should not be necessary, but we are seeing
# connections getting frequently (but sporadically) interrupted here - we
# should track this down and remove these retries
def create_auth(
    user: TestUserConfig,
    refresh_fn: Callable[[AuthConfig], None],
) -> AuthConfig:
    config: AuthConfig = retry(
        lambda: _create_auth(
            user,
            refresh_fn,
        ),
        5,
        [ConnectionError, ReadTimeout],
    )
    return config


def _create_auth(
    user: TestUserConfig,
    refresh_fn: Callable[[AuthConfig], None],
) -> AuthConfig:
    if user.frontegg_host.startswith("127.0.0.1"):
        scheme = "http"
    else:
        scheme = "https"
    token = fetch_jwt(
        email=user.email,
        password=user.password,
        host=user.frontegg_host,
        scheme=scheme,
    )

    identity_url = f"{scheme}://{user.frontegg_host}/identity/resources/users/v2/me"
    response = requests.get(
        identity_url,
        headers={"authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()

    organization_id = response.json()["tenantId"]
    app_user = user.email
    app_password = make_app_password(user.frontegg_host, token, scheme)
    tls_ca_cert_path = None

    return AuthConfig(
        organization_id=organization_id,
        token=token,
        app_user=app_user,
        app_password=app_password,
        refresh_fn=refresh_fn,
        tls_ca_cert_path=tls_ca_cert_path,
    )


def update_auth(
    user: TestUserConfig,
    auth: AuthConfig,
) -> None:
    new_auth = create_auth(user, auth.refresh_fn)

    for field in fields(new_auth):
        setattr(auth, field.name, getattr(new_auth, field.name))


def make_app_password(frontegg_host: str, token: str, scheme: str) -> str:
    response = requests.post(
        f"{scheme}://{frontegg_host}/identity/resources/users/api-tokens/v1",
        json={"description": "e2e test password"},
        headers={"authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    client_id = data["clientId"].replace("-", "")
    secret = data["secret"].replace("-", "")
    return f"mzp_{client_id}{secret}"
