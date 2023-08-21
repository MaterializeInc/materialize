# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capability
from materialize.zippy.mz_capabilities import MzIsRunning


class PeekCancellation(Action):
    """Perfoms a peek cancellation."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                """
                    > DROP TABLE IF EXISTS peek_cancellation;
                    > CREATE TABLE IF NOT EXISTS peek_cancellation (f1 INTEGER);
                    > INSERT INTO peek_cancellation SELECT generate_series(1, 1000);

                    > SET statement_timeout = '10ms';

                    ! INSERT INTO peek_cancellation
                      SELECT 1 FROM peek_cancellation AS a1, peek_cancellation AS a2, peek_cancellation AS a3;
                    contains: timeout
                    """
            )
        )
