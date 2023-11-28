# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.scalability.df import df_totals_cols

CONCURRENCY = df_totals_cols.CONCURRENCY
WORKLOAD = df_totals_cols.WORKLOAD
COUNT = df_totals_cols.COUNT
TPS_DIFF = "tps_diff"
"""'TPS_OTHER - TPS_BASELINE'. Positive value, if other endpoint has more TPS (= scales better) than baseline."""
TPS_DIFF_PERC = "tps_diff_perc"
"""'TPS_DIFF / TPS_BASELINE'. Positive value, if other endpoint has more TPS (= scales better) than baseline."""
TPS_BASELINE = "baseline_tps"
TPS_OTHER = "other_tps"
INFO_BASELINE = "baseline_info"
INFO_OTHER = "other_info"
