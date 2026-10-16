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
});
