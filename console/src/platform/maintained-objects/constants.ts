// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Lookback window options (in minutes) for the maintained-objects freshness
 * filter. Capped at 24 hours to match the retention of
 * `mz_wallclock_global_lag_recent_history`.
 */
export const LOOKBACK_OPTIONS: Record<string, string> = {
  "1": "Past 1 minute",
  "5": "Past 5 minutes",
  "15": "Past 15 minutes",
  "30": "Past 30 minutes",
  "60": "Past 1 hour",
  "180": "Past 3 hours",
  "360": "Past 6 hours",
  "1440": "Past 24 hours",
};
