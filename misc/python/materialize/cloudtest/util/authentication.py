# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Callable, Optional

import requests
from requests.exceptions import ConnectionError

from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.jwt_key import fetch_jwt, make_jwt


@dataclass
class AuthConfig:
    organization_id: str
    token: str
    app_password: Optional[str]

    refresh_fn: Callable[[AuthConfig], None]

    def refresh(self) -> None:
        self.refresh_fn(self)


DEFAULT_ORG_ID = "80b1a04a-2277-11ed-a1ce-5405dbb9e0f7"


# TODO: this retry loop should not be necessary, but we are seeing
# connections getting frequently (but sporadically) interrupted here - we
# should track this down and remove these retries
def create_auth(
    args: argparse.Namespace, refresh_fn: Callable[[AuthConfig], None]
) -> AuthConfig:
    config: AuthConfig = retry(
        lambda: _create_auth(args, refresh_fn), 5, [ConnectionError]
    )
    return config


def _create_auth(
    args: argparse.Namespace, refresh_fn: Callable[[AuthConfig], None]
) -> AuthConfig:
    if args.e2e_test_user_email is not None:
        assert args.e2e_test_user_password is not None
        assert args.frontegg_host is not None

        token = fetch_jwt(
            email=args.e2e_test_user_email,
            password=args.e2e_test_user_password,
            host=args.frontegg_host,
        )

        identity_url = f"https://{args.frontegg_host}/identity/resources/users/v2/me"
        response = requests.get(
            identity_url,
            headers={"authorization": f"Bearer {token}"},
            timeout=10,
        )
        response.raise_for_status()

        organization_id = response.json()["tenantId"]
        app_password = make_app_password(args.frontegg_host, token)
    else:
        organization_id = DEFAULT_ORG_ID
        token = make_jwt(tenant_id=organization_id)
        app_password = None

    return AuthConfig(
        organization_id=organization_id,
        token=token,
        app_password=app_password,
        refresh_fn=refresh_fn,
    )


def update_auth(args: argparse.Namespace, auth: AuthConfig) -> None:
    new_auth = create_auth(args, auth.refresh_fn)
    auth.organization_id = new_auth.organization_id
    auth.token = new_auth.token
    auth.app_password = new_auth.app_password


def make_app_password(frontegg_host: str, token: str) -> str:
    response = requests.post(
        f"https://{frontegg_host}/frontegg/identity/resources/users/api-tokens/v1",
        json={"description": "e2e test password"},
        headers={"authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    client_id = data["clientId"].replace("-", "")
    secret = data["secret"].replace("-", "")
    return f"mzp_{client_id}{secret}"
