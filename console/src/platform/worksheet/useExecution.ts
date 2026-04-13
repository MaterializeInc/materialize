// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { prettyStr } from "@materializeinc/sql-pretty";
import { useAtomValue, useSetAtom } from "jotai";
import { useCallback, useEffect, useRef, useState } from "react";

import { apiClient } from "~/api/apiClient";
import { cancelQuery } from "~/api/materialize/cancelQuery";
import { MzDataType } from "~/api/materialize/types";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";

import type { WorkerToMainMessage } from "./executionWorkerTypes";
import {
  worksheetCancelAtom,
  worksheetExecuteAtom,
  worksheetExecutionAtom,
  worksheetInlineResultsAtom,
  worksheetNoticeAtom,
  worksheetResultAtom,
  worksheetSessionAtom,
} from "./store";

/** SHOW CREATE kinds that display formatted SQL in the results panel. */
export const SHOW_CREATE_KINDS = new Set([
  "show_create_view",
  "show_create_materialized_view",
  "show_create_source",
  "show_create_table",
  "show_create_sink",
  "show_create_index",
  "show_create_connection",
  "show_create_cluster",
  "show_create_type",
]);

/** Statement kinds whose results appear in the results panel rather than inline. */
export const ROW_RETURNING_KINDS = new Set([
  "select",
  "show_objects",
  "show_columns",
  "show_variable",
  "inspect_shard",
  "explain",
  "explain_plan",
  "explain_pushdown",
  "explain_timestamp",
  "explain_sink_schema",
  "explain_plan_with",
  "explain_analyze_object",
  "explain_analyze_cluster",
  "fetch",
  "copy_to",
  ...SHOW_CREATE_KINDS,
  "subscribe",
  "tail",
]);

/** Explain kinds rendered as plain text rather than a table. */
export const TEXT_EXPLAIN_KINDS = new Set([
  "explain",
  "explain_plan",
  "explain_pushdown",
  "explain_timestamp",
  "explain_sink_schema",
  "explain_plan_with",
]);

interface SocketState {
  readyForQuery: boolean;
  error: string | undefined;
  isConnected: boolean;
}

/**
 * Manages a Web Worker that owns a WebSocket connection for executing
 * one-shot SQL queries.
 *
 * Timing is measured inside the worker as time-to-first-response
 * (send → first `Rows` message) via `performance.now()`, excluding both
 * main-thread jitter and data transfer time.
 *
 * Rows are accumulated in the worker and sent in a single `postMessage`
 * on `CommandComplete`, avoiding per-row structured clones.
 *
 * Row-returning queries (SELECT, SHOW, etc.) update `worksheetResultAtom`.
 * Other statements (INSERT, CREATE, etc.) update `worksheetInlineResultsAtom`
 * to show the result inline in the editor. Database errors also appear inline.
 *
 * Supports cancel-and-replace: executing a new query while one is running
 * cancels the previous query via `pg_cancel_backend`.
 */
export function useExecution() {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const session = useAtomValue(worksheetSessionAtom);
  const setExecution = useSetAtom(worksheetExecutionAtom);
  const setResult = useSetAtom(worksheetResultAtom);
  const setNotice = useSetAtom(worksheetNoticeAtom);
  const setInlineResults = useSetAtom(worksheetInlineResultsAtom);
  const setSession = useSetAtom(worksheetSessionAtom);

  const workerRef = useRef<Worker | null>(null);
  const sessionRef = useRef(session);
  const connectionIdRef = useRef<string | null>(null);
  /** Tracks the kind and offset of the currently executing statement. */
  const currentQueryRef = useRef<{ kind: string; offset: number } | null>(null);

  const [socketState, setSocketState] = useState<SocketState>({
    readyForQuery: false,
    error: undefined,
    isConnected: false,
  });

  // Keep sessionRef in sync so the execute callback reads current values
  useEffect(() => {
    sessionRef.current = session;
  }, [session]);

  // Store setters in refs to avoid useEffect re-runs
  const settersRef = useRef({
    setExecution,
    setResult,
    setInlineResults,
    setSession,
    setSocketState,
    setNotice,
  });
  useEffect(() => {
    settersRef.current = {
      setExecution,
      setResult,
      setInlineResults,
      setSession,
      setNotice,
      setSocketState,
    };
  }, [setExecution, setResult, setInlineResults, setSession, setNotice]);

  // Initialize worker — only recreate when httpAddress changes
  useEffect(() => {
    const currentSession = sessionRef.current;
    const worker = new Worker(
      new URL("./execution.worker.ts", import.meta.url),
      { type: "module" },
    );

    worker.postMessage({
      type: "connect",
      url: `${apiClient.mzWebsocketUrlScheme}://${httpAddress}/api/experimental/sql`,
      authConfig: apiClient.getWsAuthConfig(),
      sessionVariables: {
        cluster: currentSession.cluster,
        database: currentSession.database,
        search_path: currentSession.searchPath,
      },
    });

    worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      const setters = settersRef.current;

      switch (msg.type) {
        case "stateChange":
          setters.setSocketState((prev) => {
            if (
              prev.readyForQuery === msg.readyForQuery &&
              prev.error === msg.error &&
              prev.isConnected === msg.isConnected
            ) {
              return prev;
            }
            return {
              readyForQuery: msg.readyForQuery,
              error: msg.error,
              isConnected: msg.isConnected,
            };
          });
          break;

        case "backendKeyData":
          connectionIdRef.current = `${msg.connId}`;
          break;

        case "commandStarting":
          if (msg.isStreaming) {
            setters.setExecution((prev) => {
              if (prev.status === "running") {
                return {
                  status: "streaming",
                  statementIndex: prev.statementIndex,
                };
              }
              return prev;
            });
          }
          break;

        case "queryResult": {
          const query = currentQueryRef.current;
          if (!query) break;

          const { durationMs } = msg;

          if (SHOW_CREATE_KINDS.has(query.kind)) {
            const objectName = String(msg.rows[0]?.[0] ?? "");
            const rawSql = String(msg.rows[0]?.[1] ?? "");
            let formattedSql: string;
            try {
              formattedSql = prettyStr(rawSql, 100);
            } catch {
              formattedSql = rawSql;
            }
            setters.setResult({
              columns: [
                {
                  name: "sql",
                  type_oid: MzDataType.text,
                  type_len: -1,
                  type_mod: -1,
                },
              ],
              rows: [[formattedSql]],
              commandComplete: msg.commandComplete,
              durationMs,
              displayMode: "sql",
              kind: query.kind,
              objectName,
            });
          } else if (TEXT_EXPLAIN_KINDS.has(query.kind)) {
            const text = msg.rows.map((row) => String(row[0] ?? "")).join("\n");
            setters.setResult({
              columns: [
                {
                  name: "text",
                  type_oid: MzDataType.text,
                  type_len: -1,
                  type_mod: -1,
                },
              ],
              rows: [[text]],
              commandComplete: msg.commandComplete,
              durationMs,
              displayMode: "text",
            });
          } else if (ROW_RETURNING_KINDS.has(query.kind)) {
            setters.setResult({
              columns: msg.columns,
              rows: msg.rows,
              commandComplete: msg.commandComplete,
              durationMs,
            });
          } else {
            setters.setInlineResults((prev) => {
              const next = new Map(prev);
              next.set(query.offset, {
                kind: "success",
                message: msg.commandComplete,
              });
              return next;
            });
          }
          break;
        }

        case "readyForQuery":
          currentQueryRef.current = null;
          setters.setExecution({ status: "idle" });
          setters.setNotice((prev) => (prev === null ? prev : null));
          break;

        case "notice":
          setters.setNotice(msg.message);
          break;

        case "parameterStatus":
          if (msg.name === "cluster") {
            setters.setSession((prev) => ({
              ...prev,
              cluster: msg.value,
            }));
          } else if (msg.name === "database") {
            setters.setSession((prev) => ({
              ...prev,
              database: msg.value,
            }));
          } else if (msg.name === "search_path") {
            setters.setSession((prev) => ({
              ...prev,
              searchPath: msg.value,
            }));
          }
          break;

        case "error": {
          const query = currentQueryRef.current;
          if (query) {
            setters.setInlineResults((prev) => {
              const next = new Map(prev);
              next.set(query.offset, {
                kind: "error",
                message: msg.error.message,
              });
              return next;
            });
          }
          const errorParts = [msg.error.message];
          if (msg.error.detail) errorParts.push(msg.error.detail);
          if (msg.error.hint) errorParts.push(`HINT: ${msg.error.hint}`);
          setters.setResult({
            columns: [
              {
                name: "error",
                type_oid: MzDataType.text,
                type_len: -1,
                type_mod: -1,
              },
            ],
            rows: [[errorParts.join("\n\n")]],
            commandComplete: "ERROR",
            durationMs: msg.durationMs,
            displayMode: "text",
          });
          setters.setExecution({ status: "idle" });
          currentQueryRef.current = null;
          break;
        }

        case "setComplete":
          // SET/RESET wrapper completed — nothing to do on the main thread.
          break;
      }
    };

    worker.onerror = () => {
      settersRef.current.setSocketState({
        readyForQuery: false,
        error: "Worker error",
        isConnected: false,
      });
    };

    workerRef.current = worker;

    return () => {
      worker.terminate();
    };
  }, [httpAddress]);

  /**
   * Sends a SQL statement for execution. `offset` is the byte offset of the
   * statement in the editor, used to position inline result decorations.
   */
  const execute = useCallback(
    async (
      sql: string,
      kind: string,
      offset = 0,
      options?: { cluster?: string; replica?: string },
    ) => {
      const worker = workerRef.current;
      if (!worker) return;

      // Clear previous state
      setInlineResults((prev) => (prev.size === 0 ? prev : new Map()));
      setNotice((prev) => (prev === null ? prev : null));

      // Cancel-and-replace: cancel running query and wait for the worker
      // to become ready before sending the new one.
      if (currentQueryRef.current && connectionIdRef.current) {
        try {
          await cancelQuery({
            params: { connectionId: connectionIdRef.current },
            getConnectionIdFromSessionIdQueryKey: ["worksheet-cancel-lookup"],
            cancelQueryQueryKey: ["worksheet-cancel"],
          });
          // Wait for the cancelled query's error/ReadyForQuery to arrive
          // so the worker is free to accept a new query.
          await new Promise<void>((resolve) => {
            const onMessage = (event: MessageEvent<WorkerToMainMessage>) => {
              if (
                event.data.type === "stateChange" &&
                event.data.readyForQuery
              ) {
                worker.removeEventListener("message", onMessage);
                clearTimeout(timeout);
                resolve();
              }
            };
            const timeout = setTimeout(() => {
              worker.removeEventListener("message", onMessage);
              // Worker still busy after 5s — force reconnect
              connectionIdRef.current = null;
              worker.postMessage({ type: "reconnect" });
              resolve();
            }, 5000);
            worker.addEventListener("message", onMessage);
          });
        } catch {
          // Best-effort cancel
        }
      }

      currentQueryRef.current = { kind, offset };

      setExecution({ status: "running", statementIndex: offset });

      const queries: { query: string }[] = [];
      const restoreCluster = options?.cluster
        ? sessionRef.current.cluster
        : null;
      if (options?.cluster) {
        queries.push({ query: `SET cluster = ${options.cluster}` });
      }
      if (options?.replica) {
        queries.push({ query: `SET cluster_replica = '${options.replica}'` });
      }
      queries.push({ query: sql });
      if (options?.replica) {
        queries.push({ query: `RESET cluster_replica` });
      }
      if (restoreCluster) {
        queries.push({ query: `SET cluster = ${restoreCluster}` });
      }
      worker.postMessage({ type: "send", request: { queries } });
    },
    [setExecution, setInlineResults, setNotice],
  );

  const cancel = useCallback(async () => {
    if (connectionIdRef.current) {
      try {
        await cancelQuery({
          params: { connectionId: connectionIdRef.current },
          getConnectionIdFromSessionIdQueryKey: ["worksheet-cancel-lookup"],
          cancelQueryQueryKey: ["worksheet-cancel"],
        });
        return;
      } catch {
        // pg_cancel_backend failed — fall through to reconnect
      }
    }
    // Force-reset: reconnect the WebSocket inside the worker, which kills
    // the running query server-side.
    const worker = workerRef.current;
    if (worker) {
      currentQueryRef.current = null;
      setExecution({ status: "idle" });
      setResult(null);
      worker.postMessage({ type: "reconnect" });
    }
  }, [setExecution, setResult]);

  // Expose execute and cancel to global components via atoms.
  const setExecute = useSetAtom(worksheetExecuteAtom);
  const setCancel = useSetAtom(worksheetCancelAtom);
  useEffect(() => {
    setExecute(() => execute);
    return () => setExecute(null);
  }, [execute, setExecute]);
  useEffect(() => {
    setCancel(() => cancel);
    return () => setCancel(null);
  }, [cancel, setCancel]);

  return {
    execute,
    cancel,
    isConnected: socketState.isConnected,
    isReady: socketState.readyForQuery,
    error: socketState.error,
  };
}
