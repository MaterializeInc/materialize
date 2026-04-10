// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";
import { atomWithStorage } from "jotai/utils";

import type { Column } from "~/api/materialize/types";

import {
  LOCAL_STORAGE_CONTENT_KEY,
  LOCAL_STORAGE_SESSION_KEY,
} from "./constants";

/** Editor text, persisted to localStorage. */
export const worksheetContentAtom = atomWithStorage(
  LOCAL_STORAGE_CONTENT_KEY,
  "",
);

/** Active cluster, database, and schema for the worksheet session. */
export interface WorksheetSession {
  cluster: string;
  database: string;
  searchPath: string;
}

/** Session context persisted to localStorage. Updated by ParameterStatus messages from the server. */
export const worksheetSessionAtom = atomWithStorage<WorksheetSession>(
  LOCAL_STORAGE_SESSION_KEY,
  { cluster: "quickstart", database: "materialize", searchPath: "public" },
);

/**
 * A parsed SQL statement's position in the editor.
 * Produced by the WASM parser and used by CodeLens and diagnostics hooks.
 */
export interface StatementInfo {
  /** Statement type label from the parser (e.g. "select", "insert", "subscribe"). */
  kind: string;
  /** The SQL text of this statement. */
  sql: string;
  /** Byte offset of this statement in the full editor content. */
  offset: number;
  /** Cluster specified in the statement (e.g. IN CLUSTER), if any. */
  inCluster: string | null;
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

/** Parsed statement boundaries from the WASM parser. */
export const worksheetStatementsAtom = atom<StatementInfo[]>([]);

/**
 * Tracks whether a query is currently running or streaming.
 *
 * - `idle`: no active query.
 * - `running`: a one-shot query has been sent, waiting for results.
 * - `streaming`: the server indicated the query is streaming (e.g. SUBSCRIBE).
 */
export type ExecutionState =
  | { status: "idle" }
  | { status: "running"; statementIndex: number }
  | { status: "streaming"; statementIndex: number };

/** Current execution state. */
export const worksheetExecutionAtom = atom<ExecutionState>({ status: "idle" });

/** Result from a row-returning query (SELECT, SHOW, EXPLAIN, etc.). */
export interface QueryResult {
  columns: Column[];
  rows: unknown[][];
  /** The server's command complete tag (e.g. "SELECT 42"). */
  commandComplete: string;
  durationMs: number;
  /** How to render this result. When omitted, the results panel renders as a table. */
  displayMode?: "table" | "sql" | "text";
  /** The parser statement kind that produced this result (e.g. "show_create_materialized_view"). */
  kind?: string;
  /** For SHOW CREATE results: the qualified object name returned by the server. */
  objectName?: string;
}

/** The most recent row-returning query result, shown in the results panel. */
export const worksheetResultAtom = atom<QueryResult | null>(null);

/** Stashed SQL result to return to after viewing an EXPLAIN plan. */
export const worksheetStashedSqlResultAtom = atom<QueryResult | null>(null);

/** Execute function exposed by useExecution, used by FloatingResultPanel to run queries like EXPLAIN. */
export const worksheetExecuteAtom = atom<
  ((sql: string, kind: string, offset: number) => void) | null
>(null);

/**
 * An inline annotation displayed below a statement in the editor.
 * Success results show the command complete tag; errors show the database error message.
 */
export interface InlineResult {
  kind: "success" | "error";
  message: string;
}

/** Inline results keyed by statement byte offset. Rendered as Monaco ViewZones. */
export const worksheetInlineResultsAtom = atom<Map<number, InlineResult>>(
  new Map(),
);

/** A parse error from the WASM parser, with editor position info for diagnostics. */
export interface ParseErrorInfo {
  message: string;
  /** Byte offset in the editor content. */
  offset: number;
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

/** Current parse error, if any. Drives red squiggle markers in the editor. */
export const worksheetParseErrorAtom = atom<ParseErrorInfo | null>(null);

/** Whether the Quickstart tutorial sidebar is visible. Persisted to localStorage. */
export const tutorialVisibleAtom = atomWithStorage(
  "worksheet-tutorial-visible",
  true,
);

/** The current step index in the Quickstart tutorial. Persisted to localStorage. */
export const currentTutorialStepAtom = atomWithStorage(
  "worksheet-tutorial-step",
  0,
);

/** Whether the results panel below the editor is open. */
export const resultsPanelOpenAtom = atom(false);

/** Current height in pixels of the floating result panel. Used by the editor to add bottom padding. */
export const resultsPanelHeightAtom = atom(0);

/** The pathname where the results panel was last opened. Used to dismiss on navigation. */
export const resultsPanelPathAtom = atom<string | null>(null);

/** State for an active or completed SUBSCRIBE query. */
export interface SubscribeState {
  columns: Column[];
  materializedRows: unknown[][];
  diffRows: unknown[][];
  progressTimestamp: string | null;
  isStreaming: boolean;
  error: string | null;
}

export const INITIAL_SUBSCRIBE_STATE: SubscribeState = {
  columns: [],
  materializedRows: [],
  diffRows: [],
  progressTimestamp: null,
  isStreaming: false,
  error: null,
};

/** Global subscribe state, shared between the worksheet and the floating result panel. */
export const subscribeStateAtom = atom<SubscribeState>(INITIAL_SUBSCRIBE_STATE);

/** Callback to stop an active SUBSCRIBE. Set by the worksheet's useSubscribe hook. */
export const stopSubscribeAtom = atom<(() => void) | null>(null);
