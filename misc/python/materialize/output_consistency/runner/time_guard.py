# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from datetime import datetime, timedelta


class TimeGuard:
    def __init__(
        self,
        max_runtime_in_sec: int,
    ):
        self.max_runtime_in_sec = max_runtime_in_sec
        self.start_time = datetime.now()
        self.end_time: datetime | None = (
            self.start_time + timedelta(seconds=max_runtime_in_sec)
            if max_runtime_in_sec > 0
            else None
        )
        self.replied_abort_yes = False

    def shall_abort(self) -> bool:
        if self.end_time is None:
            return False

        if datetime.now() >= self.end_time:
            self.replied_abort_yes = True
            return True

        return False
