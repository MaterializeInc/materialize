# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from typing import List, Tuple

import pg8000

from materialize.parallel_workload.action import Action


class Worker:
    rng: random.Random
    actions: List[Tuple[Action, int]]
    end_time: float
    num_queries: int
    autocommit: bool

    def __init__(
        self,
        rng: random.Random,
        actions: List[Tuple[Action, int]],
        end_time: float,
        autocommit: bool,
    ):
        self.rng = rng
        self.actions = actions
        self.end_time = end_time
        self.num_queries = 0
        self.autocommit = autocommit

    def run(self, host: str, port: int, database: str) -> None:
        self.conn = pg8000.connect(
            host=host, port=port, user="materialize", database=database
        )
        # TODO: Transaction isolation level
        # if self.rng.choice([True, False]):
        self.conn.autocommit = self.autocommit

        try:
            with self.conn.cursor() as cur:
                cur.execute("SET TRANSACTION_ISOLATION TO serializable")
                while time.time() < self.end_time:
                    action = self.rng.choices(
                        [action[0] for action in self.actions],
                        [action[1] for action in self.actions],
                    )[0]
                    action.run(cur)
                    self.num_queries += 1
        except Exception as e:
            print(str(e))
            raise
