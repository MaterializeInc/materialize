# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pandas as pd

from materialize.scalability.workload import Workload


class WorkloadResult:
    def __init__(
        self, workload: Workload, df_totals: pd.DataFrame, df_details: pd.DataFrame
    ):
        self.workload = workload
        self.df_totals = df_totals
        self.df_details = df_details
