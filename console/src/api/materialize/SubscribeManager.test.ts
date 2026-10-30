// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildColumns } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  MockSubscribeConnection,
  mockSubscribeSocket,
} from "~/test/mockSubscribeSocket";

import { SubscribeManager } from "./SubscribeManager";

const REQUEST = {
  queries: [{ query: "SUBSCRIBE t", params: [] }],
};

/** Serves one row per subscribe, and returns each query's connection. */
const serve = () => {
  const queries: MockSubscribeConnection[] = [];
  server.use(
    mockSubscribeSocket((_sql, _params, connection) => {
      queries.push(connection);
      return { columns: buildColumns(["id"]), rows: [["a"]], timestamp: 10 };
    }),
  );
  return queries;
};

const managers: Array<SubscribeManager<{ id: string }, { id: string }>> = [];

const connect = (closeSocketOnComplete = false) => {
  const manager = new SubscribeManager<{ id: string }, { id: string }>({
    httpAddress: "subscribe.test",
    request: REQUEST,
    upsert: { key: (row) => row.data.id },
    select: (row) => row.data,
    closeSocketOnComplete,
  });
  managers.push(manager);
  manager.connect();
  return manager;
};

describe("SubscribeManager", () => {
  afterEach(() => {
    for (const manager of managers.splice(0)) manager.disconnect();
  });

  it("applies the waiting updates once when a listener reads the upper", async () => {
    const queries = serve();
    const manager = connect();
    const uppers: Array<number | undefined> = [];
    manager.onChange(() => uppers.push(manager.getUpper()));
    await vi.waitFor(() =>
      expect(manager.getSnapshot().snapshotComplete).toBe(true),
    );

    queries[0].advance(20, [["b"]]);
    queries[0].advance(30);

    await vi.waitFor(() => expect(manager.getUpper()).toBe(30));
    expect(manager.getSnapshot().data).toEqual([{ id: "a" }, { id: "b" }]);
    expect(uppers.at(-1)).toBe(30);
  });

  it("gives a listener told the snapshot completed the snapshot's upper", async () => {
    serve();
    const manager = connect();
    let upperAtCompletion: number | undefined;
    manager.onChange(() => {
      if (manager.getSnapshot().snapshotComplete) {
        upperAtCompletion ??= manager.getUpper();
      }
    });

    await vi.waitFor(() =>
      expect(manager.getSnapshot().snapshotComplete).toBe(true),
    );

    expect(upperAtCompletion).toBe(11);
  });

  it("reports a statement that ends on its own and closes its socket, without sending it again", async () => {
    const queries = serve();
    const manager = connect(true);
    await vi.waitFor(() =>
      expect(manager.getSnapshot().snapshotComplete).toBe(true),
    );

    queries[0].complete();

    await vi.waitFor(() => expect(manager.getSnapshot().isComplete).toBe(true));
    await vi.waitFor(() => expect(queries[0].isClosed).toBe(true));
    expect(manager.getSnapshot().error).toBeUndefined();
    expect(queries).toHaveLength(1);
  });

  it("follows a subscribe the server ends and runs again, dropping its error and old rows", async () => {
    const rowsByRun = [[["a"], ["b"]], [["c"]]];
    let run = 0;
    const queries: MockSubscribeConnection[] = [];
    server.use(
      mockSubscribeSocket((_sql, _params, connection) => {
        queries.push(connection);
        return {
          columns: buildColumns(["id"]),
          rows: rowsByRun[Math.min(run++, rowsByRun.length - 1)],
          timestamp: 10,
        };
      }),
    );
    const manager = connect();
    await vi.waitFor(() =>
      expect(manager.getSnapshot().data).toEqual([{ id: "a" }, { id: "b" }]),
    );

    queries[0].endWithError("target replica failed or was dropped");

    await vi.waitFor(() =>
      expect(manager.getSnapshot().data).toEqual([{ id: "c" }]),
    );
    expect(manager.getSnapshot().error).toBeUndefined();
    expect(manager.getSnapshot().snapshotComplete).toBe(true);
  });

  it("keeps reporting the error of a subscribe that fails every time it runs", async () => {
    const queries: MockSubscribeConnection[] = [];
    server.use(
      mockSubscribeSocket((_sql, _params, connection) => {
        queries.push(connection);
        if (queries.length > 1) {
          connection.error("unknown catalog item");
          return undefined;
        }
        return { columns: buildColumns(["id"]), rows: [["a"]], timestamp: 10 };
      }),
    );
    const manager = connect();
    const errors: Array<string | undefined> = [];
    manager.onChange(() => errors.push(manager.getSnapshot().error?.message));
    await vi.waitFor(() =>
      expect(manager.getSnapshot().snapshotComplete).toBe(true),
    );

    queries[0].endWithError("unknown catalog item");

    await vi.waitFor(() => expect(queries).toHaveLength(2));
    await vi.waitFor(() =>
      expect(manager.getSnapshot().error?.message).toBe("unknown catalog item"),
    );
    // No update in between drops the error.
    expect(errors.slice(errors.indexOf("unknown catalog item"))).not.toContain(
      undefined,
    );
  });
});
