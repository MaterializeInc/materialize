# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Build:
    number: str
    pipeline: str
    state: str
    branch: str
    web_url: str
    created_at: datetime
