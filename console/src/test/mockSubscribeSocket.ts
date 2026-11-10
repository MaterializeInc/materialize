// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ws } from "msw";

import { Column, MzDataType } from "~/api/materialize/types";
import { buildColumn } from "~/api/mocks/buildSqlQueryHandler";

export interface SubscribeSnapshot {
  columns: Column[];
  /** Values in `columns` order. */
  rows: unknown[][];
  /** The snapshot's timestamp. Defaults to 1. */
  timestamp?: number;
}

/** One client session, as the server side of the socket sees it. */
export interface MockSubscribeConnection {
  /** The session's `pg_backend_pid()`, which it reports on connecting. */
  connectionId: number;
  /** The session variables the client connected with. */
  options: Record<string, unknown>;
  isClosed: boolean;
  /**
   * Sends `rows` as upserts and `deletes` as deletes at the subscribe's open
   * timestamp, then the progress row that closes it at `upper`.
   */
  advance: (upper: number, rows?: unknown[][], deletes?: unknown[][]) => void;
  /** Fails the running statement, as for a relation the server doesn't have. */
  error: (message: string) => void;
  /**
   * Fails the running statement and readies the session for the next one, as
   * the server does when a subscribe's target replica restarts.
   */
  endWithError: (message: string) => void;
  /** Ends the running statement, as a subscribe does at its `UP TO`. */
  complete: () => void;
  /** Closes the socket from the server's side. */
  close: () => void;
}

const METADATA_COLUMNS = [
  buildColumn({ name: "mz_timestamp", type_oid: MzDataType.numeric }),
  buildColumn({ name: "mz_progressed", type_oid: MzDataType.bool }),
  buildColumn({ name: "mz_state", type_oid: MzDataType.text }),
];

let nextConnectionId = 1;

/**
 * Answers each SUBSCRIBE sent over the WebSocket with one snapshot of `upsert`
 * rows and the progress row that completes it, then stays silent until the
 * connection's `advance`.
 * `snapshotFor` picks a query's rows from its SQL and parameters, or returns a
 * promise of them to hold the snapshot back. A query it returns undefined for
 * never completes, like a subscribe still loading.
 */
export function mockSubscribeSocket(
  snapshotFor: (
    sql: string,
    params: unknown[],
    connection: MockSubscribeConnection,
  ) => SubscribeSnapshot | undefined | Promise<SubscribeSnapshot | undefined>,
) {
  return ws.link(/^wss?:\/\//).addEventListener("connection", ({ client }) => {
    let hasOptions = false;
    let columns: Column[] | undefined;
    let upper = 0;
    const send = (message: unknown) => client.send(JSON.stringify(message));
    const progressRow = (timestamp: number) => [
      String(timestamp),
      true,
      null,
      ...(columns ?? []).map(() => null),
    ];
    const connection: MockSubscribeConnection = {
      connectionId: nextConnectionId++,
      options: {},
      isClosed: false,
      advance: (nextUpper, rows = [], deletes = []) => {
        if (connection.isClosed) return;
        for (const row of rows) {
          send({
            type: "Row",
            payload: [String(upper), false, "upsert", ...row],
          });
        }
        for (const row of deletes) {
          send({
            type: "Row",
            payload: [String(upper), false, "delete", ...row],
          });
        }
        upper = nextUpper;
        send({ type: "Row", payload: progressRow(upper) });
      },
      error: (message) =>
        send({ type: "Error", payload: { message, code: "XX000" } }),
      endWithError: (message) => {
        send({ type: "Error", payload: { message, code: "XX000" } });
        send({ type: "ReadyForQuery", payload: "I" });
      },
      complete: () => {
        send({ type: "CommandComplete", payload: "SUBSCRIBE" });
        send({ type: "ReadyForQuery", payload: "I" });
      },
      close: () => client.close(),
    };
    client.addEventListener("close", () => {
      connection.isClosed = true;
    });
    client.addEventListener("message", async (event) => {
      // The first message carries the session options.
      if (!hasOptions) {
        hasOptions = true;
        connection.options =
          (
            JSON.parse(String(event.data)) as {
              options?: Record<string, unknown>;
            }
          ).options ?? {};
        send({
          type: "BackendKeyData",
          payload: { conn_id: connection.connectionId, secret_key: 0 },
        });
        send({ type: "ReadyForQuery", payload: "I" });
        return;
      }
      const request = JSON.parse(String(event.data)) as {
        queries: Array<{ query: string; params?: unknown[] }>;
      };
      const [query] = request.queries;
      const snapshot = await snapshotFor(
        query?.query ?? "",
        query?.params ?? [],
        connection,
      );
      if (!snapshot) return;
      columns = snapshot.columns;
      const timestamp = snapshot.timestamp ?? 1;
      send({
        type: "Rows",
        payload: { columns: [...METADATA_COLUMNS, ...snapshot.columns] },
      });
      // The manager counts the snapshot complete at the first progress row
      // after a timestamp has opened.
      send({ type: "Row", payload: progressRow(timestamp) });
      for (const row of snapshot.rows) {
        send({
          type: "Row",
          payload: [String(timestamp), false, "upsert", ...row],
        });
      }
      upper = timestamp + 1;
      send({ type: "Row", payload: progressRow(upper) });
    });
  });
}
