#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import mzbuild, spawn
from materialize.xcompile import Arch


def main() -> None:
    repo = mzbuild.Repository(Path("."))

    # Build and push any images that are not already available on Docker Hub,
    # so they are accessible to other build agents.
    print("--- Acquiring mzbuild images")
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.ensure()
    annotate_buildkite_with_tags(repo.rd.arch, deps)


def annotate_buildkite_with_tags(arch: Arch, deps: mzbuild.DependencySet) -> None:
    tags = "\n".join([f"* `{dep.spec()}`" for dep in deps])
    markdown = f"""<details><summary>{arch} Docker tags produced in this build</summary>

{tags}
</details>"""
    spawn.runv(
        ["buildkite-agent", "annotate", "--style=info", f"--context=build-{arch}"],
        stdin=markdown.encode(),
    )


if __name__ == "__main__":
    main()
