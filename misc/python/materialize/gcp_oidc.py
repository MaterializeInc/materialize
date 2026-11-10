# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""GCP credentials for Buildkite jobs through OIDC and workload identity
federation."""

import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from materialize import spawn


class BuildkiteOidcToken:
    """Keeps a Buildkite OIDC token for `audience` fresh in a file.

    GCP reads the token from the file whenever it exchanges it for an access
    token, and it exchanges again each time an access token expires (after
    1 h). The job runs longer than that, so a daemon thread rewrites the file
    well before each token expires.
    """

    LIFETIME_SECONDS = 900
    REFRESH_SECONDS = 240

    def __init__(self, audience: str, path: Path):
        self.audience = audience
        self.path = path

    def start(self) -> None:
        # The first request fails loudly: without a token nothing works.
        self._refresh()
        threading.Thread(target=self._refresh_forever, daemon=True).start()

    def _refresh(self) -> None:
        token = spawn.capture(
            [
                "buildkite-agent",
                "oidc",
                "request-token",
                "--audience",
                self.audience,
                "--lifetime",
                str(self.LIFETIME_SECONDS),
            ]
        ).strip()
        # Readers must never see a partially written token.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(token)
        tmp.replace(self.path)

    def _refresh_forever(self) -> None:
        while True:
            time.sleep(self.REFRESH_SECONDS)
            try:
                self._refresh()
            except subprocess.CalledProcessError as e:
                print(f"Could not refresh the Buildkite OIDC token: {e}")


def authenticate() -> Path:
    """Points Google's default credentials at GCP_SERVICE_ACCOUNT_EMAIL,
    impersonated through Buildkite OIDC and workload identity federation.

    Sets GOOGLE_APPLICATION_CREDENTIALS and returns the credential config's
    path. The token stays fresh only while this process lives.
    """
    provider = os.environ["GCP_WORKLOAD_IDENTITY_PROVIDER"]
    service_account = os.environ["GCP_SERVICE_ACCOUNT_EMAIL"]
    audience = f"//iam.googleapis.com/{provider}"
    directory = Path(tempfile.mkdtemp(prefix="gcp-oidc-"))
    token_path = directory / "token"
    BuildkiteOidcToken(audience, token_path).start()
    config_path = directory / "credentials.json"
    config_path.write_text(
        json.dumps(
            {
                "type": "external_account",
                "audience": audience,
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
                + f"{service_account}:generateAccessToken",
                "credential_source": {
                    "file": str(token_path),
                    "format": {"type": "text"},
                },
            }
        )
    )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(config_path)
    return config_path
