# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
from textwrap import dedent

import requests

from materialize.cloudtest.util.common import retry

LOGGER = logging.getLogger(__name__)


def fetch_jwt(
    email: str, password: str, host: str, scheme: str, max_tries: int = 10
) -> str:
    def fetch():
        res = requests.post(
            f"{scheme}://{host}/identity/resources/auth/v1/user",
            json={"email": email, "password": password},
            timeout=10,
        )
        res.raise_for_status()
        return res

    try:
        res = retry(fetch, max_tries, [requests.exceptions.HTTPError])
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
