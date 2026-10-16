// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";

import {
  buildSessionVariables,
  mapColumnToColumnMetadata,
  mapRowToObject,
} from ".";
import { buildExtendedRequestQuery } from "./executeSqlV2";
import { MaterializeWebsocket, WebSocketResult } from "./MaterializeWebsocket";
import { ColumnMetadata, SessionVariables } from "./types";

class WebsocketQueryError extends Error {}

/**
 * Runs queries one at a time over a single WebSocket session.
 *
 * Every HTTP request opens a new session, and each session start costs
 * `mz_catalog_server` about 35 ms of CPU (measured on an M.1-nano), so a
 * caller that repeats a query every few seconds should hold one of these
 * instead. Rows come back keyed and converted as `executeSqlV2` returns them.
 */
export interface WebsocketQueryClient {
  query: <T>(query: CompiledQuery<T>) => Promise<T[]>;
  close: () => void;
}

interface PendingQuery {
  resolve: (rows: unknown[]) => void;
  reject: (error: Error) => void;
  columns: ColumnMetadata[];
  rows: unknown[];
  error?: Error;
}

export function createWebsocketQueryClient({
  httpAddress,
  sessionVariables,
}: {
  httpAddress: string;
  sessionVariables?: SessionVariables;
}): WebsocketQueryClient {
  let closedError: Error | undefined;
  let pending: PendingQuery | undefined;
  let markReady: () => void = () => {};
  let failReady: (error: Error) => void = () => {};
  const ready = new Promise<void>((resolve, reject) => {
    markReady = resolve;
    failReady = reject;
  });
  // Swallows the rejection for callers that never query.
  ready.catch(() => {});
  // Chains queries so only one is in flight on the session.
  let queue: Promise<unknown> = Promise.resolve();

  const fail = (error: Error) => {
    closedError ??= error;
    failReady(closedError);
    pending?.reject(closedError);
    pending = undefined;
  };

  const handleMessage = (message: WebSocketResult) => {
    switch (message.type) {
      case "ReadyForQuery": {
        markReady();
        if (!pending) return;
        const { resolve, reject, rows, error } = pending;
        pending = undefined;
        if (error) reject(error);
        else resolve(rows);
        return;
      }
      case "Rows":
        if (pending) {
          pending.columns = message.payload.columns.map(
            mapColumnToColumnMetadata,
          );
        }
        return;
      case "Row":
        pending?.rows.push(mapRowToObject(message.payload, pending.columns));
        return;
      case "Error":
        if (pending) {
          pending.error = new WebsocketQueryError(message.payload.message);
        } else {
          fail(new WebsocketQueryError(message.payload.message));
        }
        return;
      default:
        return;
    }
  };

  const socket = new MaterializeWebsocket({
    httpAddress,
    sessionVariables: buildSessionVariables(sessionVariables),
    onMessage: handleMessage,
    onClose: () => fail(new WebsocketQueryError("Connection closed")),
  });
  socket.connect();

  const run = async <T>(query: CompiledQuery<T>): Promise<T[]> => {
    await ready;
    if (closedError) throw closedError;
    return new Promise<T[]>((resolve, reject) => {
      pending = {
        resolve: resolve as (rows: unknown[]) => void,
        reject,
        columns: [],
        rows: [],
      };
      socket.send({ queries: [buildExtendedRequestQuery(query)] });
    });
  };

  return {
    query: <T>(query: CompiledQuery<T>) => {
      const result = queue.then(() => run(query));
      queue = result.catch(() => {});
      return result;
    },
    close: () => {
      fail(new WebsocketQueryError("Client closed"));
      socket.disconnect();
    },
  };
}
