// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export const ERROR_NOTICE_OUTPUT_MAX_WIDTH = "1008px";
export const TABLE_PAGE_SIZE = 20;
export const JOTAI_DEBOUNCE_WAIT_MS = 100;

export const COMMAND_RESULT_MAX_SIZE_BYTES = "1000000"; // When running a query, sets the data response cap at 1MB

export const NAVBAR_HEIGHT_PX = 64;

export const TUTORIAL_WIDTH = "600px";

export const CONNECTION_LOST_NOTICE_MESSAGE =
  "The connection was interrupted. Some session state may have been lost.";

// Shown on a command that was in flight when the WebSocket closed. The socket
// has no heartbeat, so a connection that died while idle is only discovered
// when the next command is sent into it.
export const COMMAND_INTERRUPTED_MESSAGE =
  "The connection to Materialize was lost. This command may not have run. Press Retry to send it again.";
