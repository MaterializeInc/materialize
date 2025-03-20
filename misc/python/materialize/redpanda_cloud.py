# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import time
from typing import Any

import requests


def get_result(response: requests.Response) -> dict[str, Any]:
    if response.status_code not in (200, 201, 202):
        raise ValueError(
            f"Redpanda API call failed: {response.status_code} {response.text}"
        )
    result = response.json()
    print(result)
    return result


class RedpandaCluster:
    def __init__(self, token: str, dataplane_api_url: str) -> None:
        self.token = token
        self.dataplane_api_url = dataplane_api_url

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def create(self, object: str, content: dict[str, Any]) -> dict[str, Any]:
        return get_result(
            requests.post(
                f"{self.dataplane_api_url}/v1alpha1/{object}",
                json=content,
                headers=self.headers(),
            )
        )


class RedpandaCloud:
    def __init__(self) -> None:
        client_id = os.environ["REDPANDA_CLOUD_CLIENT_ID"]
        client_secret = os.environ["REDPANDA_CLOUD_CLIENT_SECRET"]

        result = get_result(
            requests.post(
                "https://auth.prd.cloud.redpanda.com/oauth/token",
                json={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "audience": "cloudv2-production.redpanda.cloud",
                    "grant_type": "client_credentials",
                },
            )
        )
        # Can't finish our test otherwise
        assert result["expires_in"] >= 3600, result
        self.token = result["access_token"]
        self.controlplane_api_url = "https://api.redpanda.com"

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def wait(self, result: dict[str, Any]) -> dict[str, Any]:
        operation_id = result["operation"]["id"]
        while True:
            time.sleep(10)
            result = get_result(
                requests.get(
                    f"{self.controlplane_api_url}/v1beta2/operations/{operation_id}",
                    headers=self.headers(),
                )
            )
            if result["operation"]["state"] == "STATE_COMPLETED":
                return result["operation"]
            if result["operation"]["state"] == "STATE_FAILED":
                raise ValueError(result)
            if result["operation"]["state"] != "STATE_IN_PROGRESS":
                raise ValueError(result)

    def create(self, object: str, content: dict[str, Any] | None) -> dict[str, Any]:
        return get_result(
            requests.post(
                f"{self.controlplane_api_url}/v1beta2/{object}",
                json=content,
                headers=self.headers(),
            )
        )

    def patch(self, object: str, content: dict[str, Any] | None) -> dict[str, Any]:
        return get_result(
            requests.patch(
                f"{self.controlplane_api_url}/v1beta2/{object}",
                json=content,
                headers=self.headers(),
            )
        )

    def get(self, object: str) -> dict[str, Any]:
        return get_result(
            requests.get(
                f"{self.controlplane_api_url}/v1beta2/{object}",
                headers=self.headers(),
            )
        )

    def delete(self, object: str, id: str) -> dict[str, Any]:
        return get_result(
            requests.delete(
                f"{self.controlplane_api_url}/v1beta2/{object}/{id}",
                headers=self.headers(),
            )
        )

    def get_cluster(self, cluster_info: dict[str, Any]) -> RedpandaCluster:
        return RedpandaCluster(self.token, cluster_info["dataplane_api"]["url"])
