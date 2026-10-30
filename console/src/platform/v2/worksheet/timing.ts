// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  describeExecutionTime,
  type ExecutionTime,
} from "~/platform/shell/executionTime";

/**
 * Tooltip for a result timed by Materialize. The round trip from the browser
 * is shown only here, because it adds network and queueing time the user
 * cannot act on.
 */
export function executionTimeTooltip(
  executionTime: ExecutionTime,
  roundTripMs: number | undefined,
): string {
  const { description } = describeExecutionTime(executionTime);
  return roundTripMs === undefined
    ? description
    : `${description} Round trip from your browser: ${roundTripMs}ms.`;
}
