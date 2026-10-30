// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** How long to wait after an editor change before re-parsing with the WASM parser (ms). */
export const PARSE_DEBOUNCE_MS = 200;

/** How long to wait after an editor change before persisting content to localStorage (ms). */
export const SAVE_DEBOUNCE_MS = 1000;

export const LOCAL_STORAGE_CONTENT_KEY = "worksheet-content";
export const LOCAL_STORAGE_SESSION_KEY = "worksheet-session";

/** Number of rows displayed per page in the paginated results table. */
export const PAGE_SIZE = 50;

/** Column names injected by SUBSCRIBE WITH (PROGRESS) that carry metadata rather than user data. */
export const METADATA_COLUMNS = new Set([
  "mz_timestamp",
  "mz_progressed",
  "mz_diff",
  "mz_state",
]);
