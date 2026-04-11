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

import { quoteIdentifier } from "~/api/materialize";
import { cancelQuery } from "~/api/materialize/cancelQuery";
import { MaterializeWebsocket } from "~/api/materialize/MaterializeWebsocket";
import { type Column, MzDataType } from "~/api/materialize/types";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";

import {
  type QueryResult,
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
 * Manages a WebSocket connection for executing one-shot SQL queries.
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

  const socketRef = useRef<MaterializeWebsocket | null>(null);
  const sessionRef = useRef(session);
  const connectionIdRef = useRef<string | null>(null);
  const currentQueryRef = useRef<{
    kind: string;
    offset: number;
    columns: Column[];
    rows: unknown[][];
    startTime: number;
  } | null>(null);

  const [socketState, setSocketState] = useState<SocketState>({
    readyForQuery: false,
    error: undefined,
    isConnected: false,
  });

  // Keep sessionRef in sync so the socket constructor reads current values
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

  // Initialize socket — only recreate when httpAddress changes
  useEffect(() => {
    const currentSession = sessionRef.current;
    const socket = new MaterializeWebsocket({
      httpAddress,
      sessionVariables: {
        cluster: currentSession.cluster,
        database: currentSession.database,
        search_path: currentSession.searchPath,
      },
      onMessage: (result) => {
        const setters = settersRef.current;
        switch (result.type) {
          case "BackendKeyData":
            if (result.payload.conn_id) {
              connectionIdRef.current = `${result.payload.conn_id}`;
            }
            break;

          case "CommandStarting":
            if (result.payload.is_streaming) {
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

          case "Rows":
            if (currentQueryRef.current) {
              currentQueryRef.current.columns = result.payload.columns;
            }
            break;

          case "Row":
            if (currentQueryRef.current) {
              currentQueryRef.current.rows.push(result.payload);
            }
            break;

          case "CommandComplete": {
            // Skip CommandComplete for SET statements sent as part of
            // multi-query execution (e.g. SET cluster before EXPLAIN ANALYZE).
            if (result.payload === "SET" || result.payload === "RESET") break;

            const query = currentQueryRef.current;
            if (query) {
              const durationMs = Date.now() - query.startTime;
              if (SHOW_CREATE_KINDS.has(query.kind)) {
                const objectName = String(query.rows[0]?.[0] ?? "");
                const rawSql = String(query.rows[0]?.[1] ?? "");
                let formattedSql: string;
                try {
                  formattedSql = prettyStr(rawSql, 100);
                } catch {
                  formattedSql = rawSql;
                }
                const queryResult: QueryResult = {
                  columns: [
                    {
                      name: "sql",
                      type_oid: MzDataType.text,
                      type_len: -1,
                      type_mod: -1,
                    },
                  ],
                  rows: [[formattedSql]],
                  commandComplete: result.payload,
                  durationMs,
                  displayMode: "sql",
                  kind: query.kind,
                  objectName,
                };
                setters.setResult(queryResult);
              } else if (TEXT_EXPLAIN_KINDS.has(query.kind)) {
                const text = query.rows
                  .map((row) => String(row[0] ?? ""))
                  .join("\n");
                const queryResult: QueryResult = {
                  columns: [
                    {
                      name: "text",
                      type_oid: MzDataType.text,
                      type_len: -1,
                      type_mod: -1,
                    },
                  ],
                  rows: [[text]],
                  commandComplete: result.payload,
                  durationMs,
                  displayMode: "text",
                };
                setters.setResult(queryResult);
              } else if (ROW_RETURNING_KINDS.has(query.kind)) {
                const queryResult: QueryResult = {
                  columns: query.columns,
                  rows: query.rows,
                  commandComplete: result.payload,
                  durationMs,
                };
                setters.setResult(queryResult);
              } else {
                setters.setInlineResults((prev) => {
                  const next = new Map(prev);
                  next.set(query.offset, {
                    kind: "success",
                    message: result.payload,
                  });
                  return next;
                });
              }
            }
            break;
          }

          case "ReadyForQuery":
            currentQueryRef.current = null;
            setters.setExecution({ status: "idle" });
            setters.setNotice(null);
            break;

          case "Notice":
            setters.setNotice(result.payload.message);
            break;

          case "ParameterStatus": {
            const { name, value } = result.payload;
            if (name === "cluster") {
              setters.setSession((prev) => ({ ...prev, cluster: value }));
            } else if (name === "database") {
              setters.setSession((prev) => ({ ...prev, database: value }));
            } else if (name === "search_path") {
              setters.setSession((prev) => ({ ...prev, searchPath: value }));
            }
            break;
          }

          case "Error": {
            const query = currentQueryRef.current;
            if (query) {
              setters.setInlineResults((prev) => {
                const next = new Map(prev);
                next.set(query.offset, {
                  kind: "error",
                  message: result.payload.message,
                });
                return next;
              });
            }
            const errorParts = [result.payload.message];
            if (result.payload.detail) errorParts.push(result.payload.detail);
            if (result.payload.hint) errorParts.push(`HINT: ${result.payload.hint}`);
            setters.setResult({
              columns: [{ name: "error", type_oid: MzDataType.text, type_len: -1, type_mod: -1 }],
              rows: [[errorParts.join("\n\n")]],
              commandComplete: "ERROR",
              durationMs: query ? Date.now() - query.startTime : 0,
              displayMode: "text",
            });
            setters.setExecution({ status: "idle" });
            currentQueryRef.current = null;
            break;
          }
        }
      },
    });

    socketRef.current = socket;
    socket.connect();

    const unsubscribe = socket.onChange(() => {
      const snapshot = socket.getSnapshot();
      setSocketState({
        readyForQuery: snapshot.readyForQuery,
        error: snapshot.error,
        isConnected: socket.isConnected(),
      });
    });

    return () => {
      unsubscribe();
      socket.disconnect();
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
      const socket = socketRef.current;
      if (!socket) return;

      // Clear previous state
      setInlineResults(new Map());
      setNotice(null);

      // Cancel-and-replace: cancel running query and wait for the socket
      // to become ready before sending the new one.
      if (currentQueryRef.current && connectionIdRef.current) {
        try {
          await cancelQuery({
            params: { connectionId: connectionIdRef.current },
            getConnectionIdFromSessionIdQueryKey: ["worksheet-cancel-lookup"],
            cancelQueryQueryKey: ["worksheet-cancel"],
          });
          // Wait for the cancelled query's error/ReadyForQuery to arrive
          // so the socket is free to accept a new query.
          await new Promise<void>((resolve) => {
            if (socket.getSnapshot().readyForQuery) {
              resolve();
              return;
            }
            const timeout = setTimeout(() => {
              unsubscribe();
              // Socket still busy after 5s — force reconnect
              connectionIdRef.current = null;
              socket.connect();
              resolve();
            }, 5000);
            const unsubscribe = socket.onChange(() => {
              if (socket.getSnapshot().readyForQuery) {
                clearTimeout(timeout);
                unsubscribe();
                resolve();
              }
            });
          });
        } catch {
          // Best-effort cancel
        }
      }

      currentQueryRef.current = {
        kind,
        offset,
        columns: [],
        rows: [],
        startTime: Date.now(),
      };

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
      socket.send({ queries });
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
    // Force-reset: disconnect and reconnect the WebSocket, which kills
    // the running query server-side.
    const socket = socketRef.current;
    if (socket) {
      currentQueryRef.current = null;
      setExecution({ status: "idle" });
      setResult(null);
      socket.connect();
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
