// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";
import { ws } from "msw";

import { Column, MzDataType } from "~/api/materialize/types";
import { buildColumn } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";

import {
  createWebsocketQueryClient,
  WebsocketQueryClient,
} from "./WebsocketQueryClient";

interface SentQuery {
  query: string;
  params: unknown[];
}

type Answer = { columns: Column[]; rows: unknown[][] } | { error: string };

const READY = { type: "ReadyForQuery", payload: "I" };

const WORKER_COLUMNS = [
  buildColumn({ name: "workerId", type_oid: MzDataType.int4 }),
  buildColumn({ name: "elapsedNs", type_oid: MzDataType.float8 }),
];

/**
 * Serves the WebSocket SQL protocol. Each query is answered on a later task,
 * so a query sent without waiting for the previous one's `ReadyForQuery`
 * arrives while that one is still open and counts as overlapping. Returning
 * "close" from `answer` closes the socket instead of answering.
 */
function serveQueries(answer: (query: SentQuery) => Answer | "close") {
  const received: SentQuery[] = [];
  const socket = { received, overlapped: false };
  let isAnswering = false;
  server.use(
    ws.link(/^wss?:\/\//).addEventListener("connection", ({ client }) => {
      let hasOptions = false;
      const send = (message: unknown) => client.send(JSON.stringify(message));
      client.addEventListener("message", (event) => {
        // The first message carries the session options.
        if (!hasOptions) {
          hasOptions = true;
          send(READY);
          return;
        }
        const {
          queries: [query],
        } = JSON.parse(String(event.data)) as { queries: SentQuery[] };
        received.push(query);
        if (isAnswering) socket.overlapped = true;
        isAnswering = true;
        const reply = answer(query);
        setTimeout(() => {
          isAnswering = false;
          if (reply === "close") {
            client.close();
            return;
          }
          if ("error" in reply) {
            send({
              type: "Error",
              payload: { message: reply.error, code: "XX000" },
            });
          } else {
            send({ type: "Rows", payload: { columns: reply.columns } });
            for (const row of reply.rows) send({ type: "Row", payload: row });
            send({ type: "CommandComplete", payload: "SELECT" });
          }
          send(READY);
        }, 10);
      });
    }),
  );
  return socket;
}

const workerRows = (rows: unknown[][]): Answer => ({
  columns: WORKER_COLUMNS,
  rows,
});

const clients: WebsocketQueryClient[] = [];

const createClient = () => {
  const client = createWebsocketQueryClient({
    httpAddress: "localhost:6876",
  });
  clients.push(client);
  return client;
};

const query = (sql: string, parameters: unknown[] = []) =>
  CompiledQuery.raw(sql, parameters) as CompiledQuery<Record<string, unknown>>;

describe("createWebsocketQueryClient", () => {
  afterEach(() => {
    for (const client of clients.splice(0)) client.close();
  });

  it("runs queued queries one at a time and resolves each with its own rows keyed by column", async () => {
    const socket = serveQueries(({ query: sql }) =>
      sql === "SELECT 1"
        ? workerRows([["0", "1.5"]])
        : workerRows([
            ["0", "2"],
            ["1", "3"],
          ]),
    );
    const client = createClient();

    const results = await Promise.all([
      client.query(query("SELECT 1")),
      client.query(query("SELECT 2")),
    ]);

    expect(results).toEqual([
      [{ workerId: 0, elapsedNs: 1.5 }],
      [
        { workerId: 0, elapsedNs: 2 },
        { workerId: 1, elapsedNs: 3 },
      ],
    ]);
    expect(socket.received.map(({ query: sql }) => sql)).toEqual([
      "SELECT 1",
      "SELECT 2",
    ]);
    expect(socket.overlapped).toBe(false);
  });

  it("rejects only the query that errored and answers the next one", async () => {
    serveQueries(({ query: sql }) =>
      sql === "SELECT 1/0"
        ? { error: "division by zero" }
        : workerRows([["0", "1"]]),
    );
    const client = createClient();

    const failed = client.query(query("SELECT 1/0"));
    const next = client.query(query("SELECT 1"));

    await expect(failed).rejects.toThrow("division by zero");
    await expect(next).resolves.toEqual([{ workerId: 0, elapsedNs: 1 }]);
  });

  it("rejects the pending query and every later one once the socket closes", async () => {
    serveQueries(() => "close");
    const client = createClient();

    await expect(client.query(query("SELECT 1"))).rejects.toThrow(
      "Connection closed",
    );
    await expect(client.query(query("SELECT 2"))).rejects.toThrow(
      "Connection closed",
    );
  });

  it("sends numeric parameters as strings", async () => {
    const socket = serveQueries(() => workerRows([]));
    const client = createClient();

    await client.query(
      query("SELECT $1::int4, $2::float8, $3", [42, 1.5, "a"]),
    );

    expect(socket.received).toEqual([
      { query: "SELECT $1::int4, $2::float8, $3", params: ["42", "1.5", "a"] },
    ]);
  });
});
