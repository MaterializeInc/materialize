# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzcompose system for Docker compositions.

For an overview of what mzcompose is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

from collections.abc import Sequence
from typing import (
    Any,
    TypedDict,
)


class ServiceHealthcheck(TypedDict, total=False):
    """Configuration for a check to determine whether the containers for this
    service are healthy."""

    test: list[str] | str
    """A specification of a command to run."""

    interval: str
    """The interval at which to run the healthcheck."""

    timeout: str
    """The maximum amount of time that the test command can run before it
    is considered failed."""

    retries: int
    """The number of consecutive healthchecks that must fail for the container
    to be considered unhealthy."""

    start_period: str
    """The period after container start during which failing healthchecks will
    not be counted towards the retry limit."""


class ServiceDependency(TypedDict, total=False):
    """Configuration for a check to determine whether the containers for this
    service are healthy."""

    condition: str
    """Condition under which a dependency is considered satisfied."""


class ServiceConfig(TypedDict, total=False):
    """The definition of a service in Docker Compose.

    This object corresponds directly to the YAML definition in a
    docker-compose.yml file, plus two mzcompose-specific attributes. Full
    details are available in [Services top-level element][ref] chapter of the
    Compose Specification.

    [ref]: https://github.com/compose-spec/compose-spec/blob/master/spec.md#services-top-level-element
    """

    mzbuild: str
    """The name of an mzbuild image to dynamically acquire before invoking
    Docker Compose.

    This is a mzcompose-extension to Docker Compose. The image must exist in
    the repository. If `mzbuild` is set, neither `build` nor `image` should be
    set.
    """

    propagate_uid_gid: bool
    """Request that the Docker image be run with the user ID and group ID of the
    host user.

    This is an mzcompose extension to Docker Compose. It is equivalent to
    passing `--user $(id -u):$(id -g)` to `docker run`. The default is `False`.
    """

    allow_host_ports: bool
    """Allow the service to map host ports in its `ports` configuration.

    This option is intended only for compositions that are meant to be run as
    background services in developer environments. Compositions that are
    isolated tests of Materialize should *not* enable this option, as it leads
    to unnecessary conflicts between compositions. Compositions that publish the
    same host port cannot be run concurrently. Instead, users should use the
    `mzcompose port` command to discover the ephemeral host port mapped to the
    desired container port, or to use `mzcompose up --preserve-ports`, which
    publishes all container ports as host ports on a per-invocation basis.
    """

    image: str
    """The name and tag of an image on Docker Hub."""

    hostname: str
    """The hostname to use.

    By default, the container's ID is used as the hostname.
    """

    extra_hosts: list[str]
    """Additional hostname mappings."""

    entrypoint: list[str]
    """Override the entrypoint specified in the image."""

    command: list[str]
    """Override the command specified in the image."""

    init: bool
    """Whether to run an init process in the container."""

    ports: Sequence[int | str]
    """Service ports to expose to the host."""

    environment: list[str]
    """Additional environment variables to set.

    Each entry must be in the form `NAME=VALUE`.

    TODO(benesch): this should accept a `dict[str, str]` instead.
    """

    depends_on: list[str] | dict[str, ServiceDependency]
    """The list of other services that must be started before this one."""

    tmpfs: list[str]
    """Paths at which to mount temporary file systems inside the container."""

    volumes: list[str]
    """Volumes to attach to the service."""

    networks: dict[str, dict[str, list[str]]]
    """Additional networks to join.

    TODO(benesch): this should use a nested TypedDict.
    """

    deploy: dict[str, dict[str, dict[str, str]]]
    """Additional deployment configuration, like resource limits.

    TODO(benesch): this should use a nested TypedDict.
    """

    ulimits: dict[str, Any]
    """Override the default ulimits for a container."""

    working_dir: str
    """Overrides the container's working directory."""

    healthcheck: ServiceHealthcheck
    """Configuration for a check to determine whether the containers for this
    service are healthy."""

    restart: str
    """Restart policy."""

    labels: dict[str, Any]
    """Container labels."""

    platform: str
    """Target platform for service to run on. Syntax: os[/arch[/variant]]"""

    publish: bool | None
    """Override whether an image is publishable. Unpublishable images can be built during normal test runs in CI."""

    stop_grace_period: str | None
    """Time to wait when stopping a container."""

    network_mode: str | None
    """Network mode."""

    user: str | None
    """The user for the container."""


class Service:
    """A Docker Compose service in a `Composition`.

    Attributes:
        name: The name of the service.
        config: The definition of the service.
    """

    def __init__(self, name: str, config: ServiceConfig) -> None:
        self.name = name
        self.config = config
