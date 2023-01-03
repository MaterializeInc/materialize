# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

from materialize.mzcompose import (
    Composition,
    Service,
    ServiceConfig,
    WorkflowArgumentParser,
)


class StandaloneMaterialized(Service):
    def __init__(
        self,
        name: str,
        image: Optional[str] = None,
        ports: List[str] = [],
    ) -> None:
        config: ServiceConfig = {
            "allow_host_ports": True,
            "ports": ports,
        }

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "materialized"

        super().__init__(name=name, config=config)


def workflow_start_two_mzs(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Starts two Mz instances from different git tags for the purpose of manually running
    RQG comparison tests.
    """
    parser.add_argument(
        "--this-tag", help="Run Materialize with this git tag on port 6875"
    )

    parser.add_argument(
        "--other-tag", help="Run Materialize with this git tag on port 16875"
    )
    args = parser.parse_args()

    with c.override(
        StandaloneMaterialized(
            name="mz_this",
            image=f"materialize/materialized:{args.this_tag}"
            if args.this_tag
            else None,
            ports=["6875:6875"],
        ),
        StandaloneMaterialized(
            name="mz_other",
            image=f"materialize/materialized:{args.other_tag}"
            if args.other_tag
            else None,
            ports=["16875:6875"],
        ),
    ):
        for mz in ["mz_this", "mz_other"]:
            c.up(mz)
            c.wait_for_materialized(service=mz)
