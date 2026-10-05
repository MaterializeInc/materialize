// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Notice } from "~/api/materialize/types";

import { formatCommandDuration } from "./timings";

export const EXECUTION_TIME_SESSION_VARIABLE = "emit_execution_time_notice";

/** SQLSTATE of the notice that carries a statement's server-side execution time. */
export const EXECUTION_TIME_NOTICE_CODE = "MZ012";

export type ExecutionTimeKind =
  "first_row" | "empty_result" | "completed" | "staged" | "committed";

export type ExecutionTime = {
  kind: ExecutionTimeKind;
  durationMs: number;
  strategy: string | null;
};

const KINDS: ReadonlySet<string> = new Set([
  "first_row",
  "empty_result",
  "completed",
  "staged",
  "committed",
]);

/**
 * Parses the JSON detail of an execution time notice. Returns null when the
 * payload is not understood, so the result shows the round trip alone.
 */
export function parseExecutionTimeNotice(notice: Notice): ExecutionTime | null {
  if (notice.code !== EXECUTION_TIME_NOTICE_CODE || !notice.detail) {
    return null;
  }
  try {
    const detail = JSON.parse(notice.detail);
    if (
      typeof detail.duration_us !== "number" ||
      typeof detail.kind !== "string" ||
      !KINDS.has(detail.kind)
    ) {
      return null;
    }
    return {
      kind: detail.kind as ExecutionTimeKind,
      durationMs: detail.duration_us / 1000,
      strategy: typeof detail.strategy === "string" ? detail.strategy : null,
    };
  } catch {
    return null;
  }
}

const KIND_LABELS: Record<ExecutionTimeKind, string> = {
  first_row: "to first row",
  empty_result: "to empty result",
  completed: "in Materialize",
  staged: "in Materialize, before commit",
  committed: "in Materialize, including commit",
};

const STRATEGY_LABELS: Record<string, string> = {
  "fast-path": "served from an index",
  "persist-fast-path": "read from storage",
  standard: "computed by a temporary dataflow",
  constant: "computed without a cluster",
};

export function formatExecutionTime(executionTime: ExecutionTime): string {
  const duration = formatCommandDuration(executionTime.durationMs);
  const strategy = executionTime.strategy
    ? STRATEGY_LABELS[executionTime.strategy]
    : undefined;
  const label = `${duration} ${KIND_LABELS[executionTime.kind]}`;
  return strategy ? `${label} (${strategy})` : label;
}

/**
 * Whether `notice` is the startup notice an environment that predates the
 * execution time notice sends for the unknown session variable.
 */
export function isUnsupportedExecutionTimeSetting(notice: Notice): boolean {
  return notice.message.startsWith(
    `startup setting ${EXECUTION_TIME_SESSION_VARIABLE} not set`,
  );
}
