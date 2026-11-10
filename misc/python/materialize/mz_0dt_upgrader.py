# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random

from materialize.mz_version import MzVersion


def generate_random_upgrade_path(
    versions: list[MzVersion],
    rng: Random | None = None,
    max_versions: int = 6,
) -> list[MzVersion]:
    """
    Generates a random upgrade path between the given versions.
    """
    selected_versions = []

    rng = rng or Random()
    # For each version in the input list, randomly select it with a 1/3 chance.
    for v in versions:
        if rng.random() < 0.33:
            selected_versions.append(v)

    # Always include at least one version to avoid empty paths.
    if len(selected_versions) == 0:
        selected_versions.append(rng.choice(versions))

    # Cap the number of versions to keep test runtime bounded.
    if len(selected_versions) > max_versions:
        selected_versions = sorted(rng.sample(selected_versions, max_versions))

    return selected_versions
