# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.cloudtest.application import Application


class CloudtestApplication(Application):
    def __init__(
        self,
        release_mode: bool = True,
        tag: Optional[str] = None,
        aws_region: Optional[str] = None,
        log_filter: Optional[str] = None,
    ) -> None:
        pass

    def create(self) -> None:
        pass

    def acquire_images(self) -> None:
        pass

    def wait_replicas(self) -> None:
        pass

    def wait_for_sql(self) -> None:
        pass

    def set_environmentd_failpoints(self, failpoints: str) -> None:
        pass
