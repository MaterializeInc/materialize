# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


MZ_PIPELINES = [
    "cleanup",
    "coverage",
    "deploy",
    "deploy-mz-lsp-server",
    "deploy-mz",
    "deploy-website",
    "license",
    "nightly",
    "qa-canary",
    "release-qualification",
    "security",
    "slt",
    "test",
    "www",
]

MZ_PIPELINES_WITH_WILDCARD = MZ_PIPELINES + ["*"]
