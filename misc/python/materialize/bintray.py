# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A Bintray API client.

Consult the [official Bintray API documentation][official-docs] for details.
Only the operations that are presently necessary are implemented.

[official-docs]: https://bintray.com/docs/api/
"""

from typing import IO, Sequence
from typing_extensions import Final
from requests import Session, Response
import requests

API_BASE: Final = "https://api.bintray.com"


class VersionAlreadyExistsError(Exception):
    """Raised when creating a version of a Bintray package that already
    exists."""


class PackageClient:
    """A client for a specific Bintray package."""

    def __init__(self, session: Session, subject: str, repo: str, package: str) -> None:
        self.session = session
        self.subject = subject
        self.repo = repo
        self.package = package

    def create_version(self, version: str, desc: str, vcs_tag: str) -> Response:
        """Create a new version of the package.

        Args:
            version: The name of the version to create, e.g. "2.0".
            desc: A description of the version.
            vcs_tag: The version control tag or commit corresponding to the
                version.
        """
        url = f"{API_BASE}/packages/{self.subject}/{self.repo}/{self.package}/versions"
        res = self.session.post(
            url, json={"name": version, "desc": desc, "vcs_tag": vcs_tag}
        )
        if res.status_code == requests.codes.conflict:
            raise VersionAlreadyExistsError()
        else:
            res.raise_for_status()
        return res

    def debian_upload(
        self,
        version: str,
        path: str,
        data: IO[bytes],
        distributions: Sequence[str],
        components: Sequence[str],
        architectures: Sequence[str],
    ) -> Response:
        """Upload a Debian artifact for a version.

        The version must already have been created with e.g.
        `PackageClient.create_version`.

        Args:
            version: The version of the package to attach the artifact to.
            path: The file path for the uploaded .deb.
            data: An IO handle to the .deb file.
            distributions: The Debian distributions for the artifact.
            components: The components in the Debian distribution to which the
                artifact belongs.
            architecture: The architectures for which the artifact was built.
        """
        url = f"{API_BASE}/content/{self.subject}/{self.repo}/{self.package}/{version}/{path}"
        res = self.session.put(
            url,
            data=data,
            headers={
                "X-Bintray-Debian-Distribution": ",".join(distributions),
                "X-Bintray-Debian-Component": ",".join(components),
                "X-Bintray-Debian-Architecture": ",".join(architectures),
            },
        )
        res.raise_for_status()
        return res

    def publish_uploads(self, version: str, wait_for_seconds: int = 0) -> Response:
        """Publish all unpublished content for a version.

        Args:
            version: The version of the package to publish content for.
            wait_for_seconds: The number of seconds to wait for files to
                publish. A value of -1 means to wait for the maximum allowable
                time. A value of 0 is the default and means to publish
                asynchronously, without waiting at all.
        """
        url = f"{API_BASE}/content/{self.subject}/{self.repo}/{self.package}/{version}/publish"
        res = self.session.post(
            url, json={"publish_wait_for_seconds": wait_for_seconds}
        )
        res.raise_for_status()
        return res


class RepoClient:
    """A client for a specific Bintray repository."""

    def __init__(self, session: Session, subject: str, repo: str):
        self.session = session
        self.subject = subject
        self.repo = repo

    def package(self, package: str) -> PackageClient:
        """Construct a client for the named package."""
        return PackageClient(self.session, self.subject, self.repo, package)


class Client:
    """A root Bintray client.

    Args:
        subject: The organization to issue requests for.
        user: The user to authenticate as.
        api_key: The API key to authenticate with.
    """

    def __init__(self, subject: str, user: str, api_key: str):
        self.session = Session()
        self.session.auth = (f"{user}", api_key)
        self.subject = subject

    def repo(self, repo: str) -> RepoClient:
        """Construct a client for the named repository."""
        return RepoClient(self.session, self.subject, repo)
