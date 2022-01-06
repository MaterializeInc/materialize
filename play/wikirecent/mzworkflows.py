# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize.mzcompose import Composition


def workflow_demo(c: Composition) -> None:
    """Streams data from Wikipedia to a browser visualzation."""
    c.start_services(services=["server"])
    c.wait_for_mz()
    c.run_sql((Path(__file__).parent / "views.sql").read_text())
