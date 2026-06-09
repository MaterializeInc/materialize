# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose` for the Antithesis workload image.

`materialize.parallel_workload` and `materialize.data_ingest` import
`materialize.mzcompose` symbols at module load time even on code paths that
don't actually run a docker-compose harness. The Antithesis workload image is
a slim Python container with no docker/mzbuild toolchain, so we ship these
stubs in its `PYTHONPATH` to satisfy the imports. Only attributes the
parallel-workload driver hits at module top are provided; anything called at
runtime in this environment would be a bug.
"""

from __future__ import annotations

from typing import Any


def get_default_system_parameters() -> dict[str, str]:
    return {}


cluster_replica_size_map: dict[str, Any] = {}


def __getattr__(name: str) -> Any:
    """Lazy re-export of `MzVersion`.

    One Check class (`all_checks/password_auth.py`) still uses the
    legacy `from materialize.mzcompose import MzVersion` import path
    that the real `materialize.mzcompose.__init__` keeps as a
    backwards-compat re-export.  Doing the import lazily here means:

      * The platform-checks workload image (which bundles
        `materialize.mz_version`) gets a working `MzVersion`.
      * Every other workload group, which doesn't ship
        `materialize.mz_version` and the `semver` pip dep it needs,
        doesn't break on package-load just because the stub re-exports
        something they never reference.

    Anything other than `MzVersion` falls through to a normal
    AttributeError, which is what Python's attribute lookup expects.
    """
    if name == "MzVersion":
        from materialize.mz_version import MzVersion

        return MzVersion
    raise AttributeError(f"module 'materialize.mzcompose' has no attribute {name!r}")


class _LoaderModule:
    pass


loader = _LoaderModule()
