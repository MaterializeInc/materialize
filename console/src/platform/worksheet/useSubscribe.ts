// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Switch to @materializeinc/sql-parser when published.
import { inject_progress } from "@sjwiesman/sql-parser";
import { useAtomValue, useSetAtom } from "jotai";
import { useCallback, useEffect, useRef } from "react";

import { MaterializeWebsocket } from "~/api/materialize/MaterializeWebsocket";
import type { Column } from "~/api/materialize/types";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";

import { METADATA_COLUMNS } from "./constants";
import {
  INITIAL_SUBSCRIBE_STATE,
  stopSubscribeAtom,
  subscribeStateAtom,
  worksheetInlineResultsAtom,
  worksheetSessionAtom,
} from "./store";

/**
 * Manages a dedicated WebSocket connection for SUBSCRIBE queries.
 *
 * Automatically injects `WITH (PROGRESS)` via the WASM parser so that
 * progress timestamps can be tracked. Rows are buffered per-timestamp and
 * flushed to state on each progress barrier, avoiding per-row re-renders.
 *
 * Returns `start(sql, offset)` to begin a subscribe, `stop()` to cancel it,
 * and `reset()` to clear all state (used when switching to a different query type).
 */
export function useSubscribe() {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const session = useAtomValue(worksheetSessionAtom);
  const setInlineResults = useSetAtom(worksheetInlineResultsAtom);
  const socketRef = useRef<MaterializeWebsocket | null>(null);
  const setState = useSetAtom(subscribeStateAtom);
  const setStopCallback = useSetAtom(stopSubscribeAtom);

  const start = useCallback(
    (sql: string, offset = 0) => {
      socketRef.current?.disconnect();
      setInlineResults(new Map());

      const subscribeSql = inject_progress(sql);
      const columns: Column[] = [];
      let currentTimestampBuffer: unknown[][] = [];
      const diffLog: unknown[][] = [];
      const materialized = new Map<string, { row: unknown[]; count: number }>();
      let querySent = false;

      // Indices of non-metadata columns, computed once after Rows message
      let dataColIndices: number[] = [];
      let mzProgressedIdx = -1;
      let mzTimestampIdx = -1;
      let mzDiffIdx = -1;

      const socket = new MaterializeWebsocket({
        httpAddress,
        sessionVariables: {
          cluster: session.cluster,
          database: session.database,
          search_path: session.searchPath,
        },
        onMessage: (result) => {
          switch (result.type) {
            case "Rows":
              columns.push(...result.payload.columns);
              mzProgressedIdx = columns.findIndex(
                (c) => c.name === "mz_progressed",
              );
              mzTimestampIdx = columns.findIndex(
                (c) => c.name === "mz_timestamp",
              );
              mzDiffIdx = columns.findIndex((c) => c.name === "mz_diff");
              dataColIndices = columns
                .map((c, i) => (METADATA_COLUMNS.has(c.name) ? -1 : i))
                .filter((i) => i >= 0);
              break;

            case "Row": {
              const isProgress =
                mzProgressedIdx >= 0 &&
                (result.payload[mzProgressedIdx] === "t" ||
                  result.payload[mzProgressedIdx] === true);

              if (isProgress) {
                // Flush buffer: update diff log and materialized view
                for (const row of currentTimestampBuffer) {
                  diffLog.push(row);
                  const key = dataColIndices
                    .map((i) => String(row[i]))
                    .join("\0");
                  const diff = mzDiffIdx >= 0 ? Number(row[mzDiffIdx]) : 1;
                  const existing = materialized.get(key);
                  const newCount = (existing?.count ?? 0) + diff;
                  if (newCount > 0) {
                    materialized.set(key, { row, count: newCount });
                  } else {
                    materialized.delete(key);
                  }
                }
                // Include the progress row in the diff log
                diffLog.push(result.payload);
                currentTimestampBuffer = [];

                const ts =
                  mzTimestampIdx >= 0
                    ? String(result.payload[mzTimestampIdx])
                    : null;

                setState((prev) => ({
                  ...prev,
                  columns,
                  progressTimestamp: ts ?? prev.progressTimestamp,
                  materializedRows: Array.from(materialized.values()).flatMap(
                    ({ row, count }) =>
                      Array.from({ length: count }, () => row),
                  ),
                  diffRows: [...diffLog],
                }));
              } else {
                currentTimestampBuffer.push(result.payload);
              }
              break;
            }

            case "Error":
              setState((prev) => ({
                ...prev,
                isStreaming: false,
                error: result.payload.message,
              }));
              setInlineResults((prev) => {
                const next = new Map(prev);
                next.set(offset, {
                  kind: "error",
                  message: result.payload.message,
                });
                return next;
              });
              break;

            case "ReadyForQuery":
              if (!querySent) {
                querySent = true;
                socket.send({ queries: [{ query: subscribeSql }] });
                setState((prev) => ({
                  ...prev,
                  isStreaming: true,
                  error: null,
                  columns: [],
                  materializedRows: [],
                  diffRows: [],
                  progressTimestamp: null,
                }));
              }
              break;
          }
        },
        onClose: () => {
          setState((prev) => ({
            ...prev,
            isStreaming: false,
          }));
        },
      });

      socketRef.current = socket;
      socket.connect();
    },
    [httpAddress, session, setInlineResults, setState],
  );

  const stop = useCallback(() => {
    socketRef.current?.disconnect();
    socketRef.current = null;
    setState((prev) => ({ ...prev, isStreaming: false }));
  }, [setState]);

  const reset = useCallback(() => {
    socketRef.current?.disconnect();
    socketRef.current = null;
    setState(INITIAL_SUBSCRIBE_STATE);
  }, [setState]);

  // Register the stop callback so the floating panel can call it
  useEffect(() => {
    setStopCallback(() => stop);
    return () => setStopCallback(null);
  }, [stop, setStopCallback]);

  useEffect(() => {
    return () => {
      socketRef.current?.disconnect();
    };
  }, []);

  return { start, stop, reset };
}
