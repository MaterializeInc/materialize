// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export const QUERY_HISTORY_LIST_TABLE = "mz_recent_activity_log" as const;
export const QUERY_HISTORY_LIST_TABLE_REDACTED =
  "mz_recent_activity_log_redacted" as const;

// We use a two minutes to tolerate environments with large query volume
export const QUERY_HISTORY_REQUEST_TIMEOUT_MS = 120_000;
