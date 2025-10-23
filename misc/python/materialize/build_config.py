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
