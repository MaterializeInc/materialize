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
}

const METADATA_COLUMNS = [
  buildColumn({ name: "mz_timestamp", type_oid: MzDataType.numeric }),
  buildColumn({ name: "mz_progressed", type_oid: MzDataType.bool }),
  buildColumn({ name: "mz_state", type_oid: MzDataType.text }),
];

/**
 * Answers each SUBSCRIBE sent over the WebSocket with one snapshot of `upsert`
 * rows and the progress row that completes it, then stays silent.
 * `snapshotFor` picks a query's rows from its SQL and parameters. A query it
 * returns undefined for never completes, like a subscribe still loading.
 */
export function mockSubscribeSocket(
  snapshotFor: (
    sql: string,
    params: unknown[],
  ) => SubscribeSnapshot | undefined,
) {
  return ws.link(/^wss?:\/\//).addEventListener("connection", ({ client }) => {
    let hasOptions = false;
    const send = (message: unknown) => client.send(JSON.stringify(message));
    client.addEventListener("message", (event) => {
      // The first message carries the session options.
      if (!hasOptions) {
        hasOptions = true;
        send({ type: "ReadyForQuery", payload: "I" });
        return;
      }
      const request = JSON.parse(String(event.data)) as {
        queries: Array<{ query: string; params?: unknown[] }>;
      };
      const [query] = request.queries;
      const snapshot = snapshotFor(query?.query ?? "", query?.params ?? []);
      if (!snapshot) return;
      const progressRow = (timestamp: string) => [
        timestamp,
        true,
        null,
        ...snapshot.columns.map(() => null),
      ];
      send({
        type: "Rows",
        payload: { columns: [...METADATA_COLUMNS, ...snapshot.columns] },
      });
      // The manager counts the snapshot complete at the first progress row
      // after a timestamp has opened.
      send({ type: "Row", payload: progressRow("1") });
      for (const row of snapshot.rows) {
        send({ type: "Row", payload: ["1", false, "upsert", ...row] });
      }
      send({ type: "Row", payload: progressRow("2") });
    });
  });
}
