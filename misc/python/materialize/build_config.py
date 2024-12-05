# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from pathlib import Path
from textwrap import dedent
from typing import Any

import toml


class BuildConfig:
    """Configuration for builds of Materialize.

    Most things should be configured via a tool's native configuration file,
    e.g. `.bazelrc` or `.cargo/config.toml`. This exists for Materialize's home
    grown tools, or to extend tools that don't support an option we need.

    Looks for configuration files in `~/.config/materialize/build.toml`
    """

    def __init__(self, path: Path):
        if path.is_file():
            with open(path) as f:
                raw_data = toml.load(f)
        else:
            raw_data = {}

        self.bazel = BazelConfig(raw_data.get("bazel", {}))

    @staticmethod
    def read():
        home = Path.home()
        path = home / ".config" / "materialize" / "build.toml"

        return BuildConfig(path)

    def __str__(self):
        return f"{self.bazel}"


class BazelConfig:
    """Configuration for Bazel builds.

    Most configuration should go into either the repositories `.bazelrc` file
    or documented to be included in a users' home `.bazelrc` file. This exists
    for flags that Bazel does not have an easy way to configure itself.

    [bazel]
    remote_cache = "localhost:6889"
    """

    def __init__(self, data: dict[str, Any]):
        self.remote_cache = data.get("remote_cache", None)
        self.remote_cache_check_interval_minutes = data.get(
            "remote_cache_check_interval_minutes", "5"
        )

    def __str__(self):
        return dedent(
            f"""
        Bazel:
            remote_cache = {self.remote_cache}
        """
        )


class LocalState:
    """Local state persisted by a tool.

    Users should not expect this state to be durable, it can be blown away at
    at point.

    Stored at: ~/.cache/materialize/build_state.toml
    """

    def __init__(self, path: Path):
        self.path = path
        if path.is_file():
            with open(path) as f:
                self.data = toml.load(f)
        else:
            self.data = {}

    @staticmethod
    def default_path() -> Path:
        home = Path.home()
        path = home / ".cache" / "materialize" / "build_state.toml"
        return path

    @classmethod
    def read(cls, namespace: str) -> Any | None:
        cache = LocalState(LocalState.default_path())
        return cache.data.get(namespace, None)

    @classmethod
    def write(cls, namespace: str, val: Any):
        cache = LocalState(LocalState.default_path())
        cache.data[namespace] = val

        Path(os.path.dirname(cache.path)).mkdir(parents=True, exist_ok=True)
        with open(cache.path, "w+") as f:
            toml.dump(cache.data, f)


class TeleportLocalState:
    def __init__(self, data: dict[str, Any] | None):
        self.data = data or {}

    @classmethod
    def read(cls):
        return TeleportLocalState(LocalState.read("teleport"))

    def write(self):
        LocalState.write("teleport", self.data)

    def get_pid(self, app_name: str) -> str | None:
        existing = self.data.get(app_name, {})
        return existing.get("pid")

    def set_pid(self, app_name: str, pid: str | None):
        existing = self.data.get(app_name, {})
        existing["pid"] = pid
        self.data[app_name] = existing

    def get_address(self, app_name: str) -> str | None:
        existing = self.data.get(app_name, {})
        return existing.get("address")

    def set_address(self, app_name: str, addr: str | None):
        existing = self.data.get(app_name, {})
        existing["address"] = addr
        self.data[app_name] = existing

    def __str__(self):
        return dedent(
            f"""
        TeleportLocalState:
            data: {self.data}
        """
        )
