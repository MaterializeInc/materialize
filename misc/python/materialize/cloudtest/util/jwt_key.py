# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import datetime
import logging
import uuid
from textwrap import dedent

import jwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from materialize.cloudtest.util.common import retry

LOGGER = logging.getLogger(__name__)


def _generate_jwt_keys() -> tuple[rsa.RSAPrivateKey, bytes]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_key, public_key_bytes


JWK_PRIVATE_KEY, JWK_PUBLIC_KEY = _generate_jwt_keys()


def make_jwt(tenant_id: str) -> str:
    """
    Build a JWT to authenticate to the environment controller. (This must be
    done dynamically in the test so that the token has a valid, recent
    timestamp).

    NOTE that the following constraints are assumed:
    - `email` must end with `materialize[dot]com`
    - `roles` must include `MaterializeAdmin`
    - `iss` must match the environment controller's `FRONTEGG_URL` variable
    """
    # JWTs expect UNIX timestamps
    now = int(datetime.datetime.now().timestamp())
    payload = {
        "sub": str(uuid.uuid4()),
        "email": "test@materialize.com",
        "roles": [
            "MaterializeAdmin",
        ],
        "permissions": [
            "materialize.environment.read",
            "materialize.environment.write",
        ],
        "tenantId": tenant_id,
        "iss": "https://cloud.materialize.com",
        "iat": now,
        "exp": now + 3600,
    }
    tok: str = jwt.encode(payload, JWK_PRIVATE_KEY, algorithm="RS256")
    return tok


def fetch_jwt(email: str, password: str, host: str) -> str:
    def fetch():
        res = requests.post(
            f"https://{host}/frontegg/identity/resources/auth/v1/user",
            json={"email": email, "password": password},
            timeout=10,
        )
        res.raise_for_status()
        return res

    try:
        res = retry(fetch, 10, [requests.exceptions.HTTPError])
    except requests.exceptions.HTTPError as e:
        res = e.response
        LOGGER.error(
            dedent(
                f"""
                e: {e}
                res: {res}
                res.text: {res.text}
                """
            )
        )
        raise

    access_token: str = res.json()["accessToken"]
    return access_token
