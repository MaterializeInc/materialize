# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.scalability.df.df_details import DfDetails
from materialize.scalability.df.df_totals import DfTotals
from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.workload.workload import Workload


class WorkloadResult:
    def __init__(
        self,
        workload: Workload,
        endpoint: Endpoint,
        df_totals: DfTotals,
        df_details: DfDetails,
    ):
        self.workload = workload
        self.endpoint = endpoint
        self.df_totals = df_totals
        self.df_details = df_details
