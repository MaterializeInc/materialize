#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import subprocess
from pathlib import Path

from materialize import ci_util, mzbuild
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


def main() -> None:
    if raw := os.getenv("CI_QPS_DIAGNOSTIC_IMAGES"):
        tag = os.environ["CI_QPS_DIAGNOSTIC_TAG"]
        assert tag.endswith(".qps-lto-nov12")
        images = json.loads(raw)
        assert set(images) == {"environmentd", "clusterd"}
        for name, sources in images.items():
            assert len(sources) == 2
            assert all(s.startswith(f"materialize/{name}:mzbuild-") for s in sources)
            destination = f"materialize/{name}:{tag}"
            assert not mzbuild.is_docker_image_pushed(destination), destination
            subprocess.run(
                ["docker", "manifest", "create", "--amend", destination, *sources],
                check=True,
            )
            subprocess.run(["docker", "manifest", "push", destination], check=True)
        return
    sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]

    repos = [
        mzbuild.Repository(
            Path("."),
            Arch.X86_64,
            coverage=False,
            sanitizer=sanitizer,
            image_registry="materialize",
        ),
        mzbuild.Repository(
            Path("."),
            Arch.AARCH64,
            coverage=False,
            sanitizer=sanitizer,
            image_registry="materialize",
        ),
    ]
    print("--- Tagging development Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]
    mzbuild.publish_multiarch_images(ci_util.dev_docker_tag(), deps)


if __name__ == "__main__":
    main()
