// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Notice } from "~/api/materialize/types";
import { capitalizeSentence } from "~/util";

import { formatCommandDuration } from "./timings";

export const EXECUTION_TIME_SESSION_VARIABLE = "emit_execution_time_notice";

/**
 * The first Materialize version that knows `emit_execution_time_notice`. The
 * Shell sets it only on environments at this version or later, because an
 * older environment answers an unknown startup setting with a notice that the
 * Shell would print.
 *
 * TODO: set to the first release that contains the server change once it has
 * merged. A version that is too high only delays the feature.
 */
export const EXECUTION_TIME_MIN_VERSION = "26.49.0-dev";

/** SQLSTATE of the notice that carries a statement's server-side execution time. */
export const EXECUTION_TIME_NOTICE_CODE = "MZ012";

const READ_HELP =
  "The time Materialize took to produce the query's result. It does not include sending the rows to your browser, which happens after Materialize has computed the whole result.";

// Only `staged` names its end point in the label: its number leaves out a
// commit, which users would otherwise assume it covers.
const KINDS = {
  first_row: { label: "in Materialize", help: READ_HELP },
  empty_result: { label: "in Materialize", help: READ_HELP },
  completed: {
    label: "in Materialize",
    help: "The time Materialize took to complete the statement.",
  },
  staged: {
    label: "in Materialize, staged",
    help: "The time Materialize took for the statement, without the commit of its transaction, which comes after it.",
  },
} as const;

export type ExecutionTimeKind = keyof typeof KINDS;

export type ExecutionTime = {
  kind: ExecutionTimeKind;
  durationMs: number;
};

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
      !Object.hasOwn(KINDS, detail.kind)
    ) {
      return null;
    }
    return {
      kind: detail.kind as ExecutionTimeKind,
      durationMs: detail.duration_us / 1000,
    };
  } catch {
    return null;
  }
}

/**
 * The execution time among a statement's notices, or null when none of them
 * carries one this Console can read.
 */
export function findExecutionTime(notices: Notice[]): ExecutionTime | null {
  for (const notice of notices) {
    const executionTime = parseExecutionTimeNotice(notice);
    if (executionTime) {
      return executionTime;
    }
  }
  return null;
}

/** The result tooltip's row that says what the execution time covers. */
export function describeExecutionTime(executionTime: ExecutionTime): {
  label: string;
  description: string;
} {
  const { label, help } = KINDS[executionTime.kind];
  return { label: capitalizeSentence(label, false), description: help };
}

export function formatExecutionTime(executionTime: ExecutionTime): string {
  // `formatCommandDuration` shows one decimal of a millisecond, which would
  // print a server time of tens of microseconds as "0.0ms".
  const duration =
    executionTime.durationMs < 0.1
      ? "<0.1ms"
      : formatCommandDuration(executionTime.durationMs);
  return `${duration} ${KINDS[executionTime.kind].label}`;
}
