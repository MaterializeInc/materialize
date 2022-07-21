# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess
import unittest
from unittest.mock import patch

from materialize import ROOT, mzbuild


class CloudTestCase(unittest.TestCase):
    def acquire_images(self) -> None:
        images = ["environmentd", "computed", "storaged", "testdrive"]

        # Direct mzbuild to push the images into minikube's container registry
        # while preserving the original values for use inside the ci-builder
        minikube_env = {
            "MZ_DEV_CI_BUILDER_DOCKER_HOST": os.environ.get("DOCKER_HOST", ""),
            "MZ_DEV_CI_BUILDER_DOCKER_TLS_VERIFY": os.environ.get(
                "DOCKER_TLS_VERIFY", ""
            ),
            "MZ_DEV_CI_BUILDER_DOCKER_CERT_PATH": os.environ.get(
                "DOCKER_CERT_PATH", ""
            ),
        }

        minikube_env_str = subprocess.check_output(["minikube", "docker-env"]).decode(
            "ascii"
        )
        prefix = "export "
        for minikube_env_line in minikube_env_str.splitlines():
            if minikube_env_line.startswith(prefix):
                parts = minikube_env_line[len(prefix) :].split("=")
                minikube_env[parts[0]] = parts[1].strip('"')

        repo = mzbuild.Repository(ROOT)
        with patch.dict("os.environ", minikube_env):
            for image in images:
                deps = repo.resolve_dependencies([repo.images[image]])
                deps.acquire()
