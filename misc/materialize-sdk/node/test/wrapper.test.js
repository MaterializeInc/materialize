// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The live subscription's error handling, over a fake connection.

const assert = require("node:assert/strict");
const { beforeEach, mock, test } = require("node:test");
const pg = require("pg");
const { Client } = pg;

const {
  LiveSubscription,
  ProtocolError,
  ServerError,
  UsageError,
} = require("..");

const COLUMNS = ["mz_timestamp", "mz_progressed", "mz_diff", "id"];
const OK = { fields: [], rows: [] };

const result = (columns, rows) => ({
  fields: columns.map((name) => ({ name })),
  rows,
});

const ROWS = [
  ["5", "t", null, null],
  ["5", "f", "1", "a"],
  ["6", "t", null, null],
];

// Answers each query with the next of `results`, rejecting with an Error one.
// `clients` collects each queried client's result format and pipelining.
function fake(results, clients = []) {
  const ended = { value: false };
  mock.method(Client.prototype, "connect", async () => {});
  mock.method(Client.prototype, "end", async () => {
    ended.value = true;
  });
  mock.method(Client.prototype, "query", async function (config) {
    clients.push({ binary: this.binary, pipeline: this.pipeline });
    const next = results.shift();
    if (next instanceof Error) {
      throw next;
    }
    // Each statement completes with the command its text starts with.
    return { command: config.text.split(" ")[0], ...next };
  });
  return ended;
}

beforeEach(() => mock.restoreAll());

test("a failure is thrown again on every later call", async () => {
  fake([OK, OK, result(["id"], [["1"]])]);
  const live = await LiveSubscription.open("", "SELECT 1");
  const first = await live.next().catch((error) => error);
  assert.ok(first instanceof ProtocolError);
  const second = await live.next().catch((error) => error);
  assert.equal(second, first);
});

test("a failed setup closes the connection", async () => {
  const ended = fake([OK, new Error("refused")]);
  await assert.rejects(LiveSubscription.open("", "SELECT 1"), ServerError);
  assert.ok(ended.value);
});

test("leaving a for await loop closes the connection", async () => {
  const ended = fake([OK, OK, result(COLUMNS, ROWS)]);
  for await (const _ of await LiveSubscription.open("", "SELECT 1")) {
    break;
  }
  assert.ok(ended.value);
});

test("timestamps and diffs are bigints", async () => {
  fake([OK, OK, result(COLUMNS, ROWS)]);
  const { value } = await (await LiveSubscription.open("", "SELECT 1")).next();
  assert.deepEqual([value.lower, value.upper], [null, 6n]);
  assert.deepEqual(value.updates, [
    { timestamp: 5n, op: "diff", row: ["a"], diff: 1n },
  ]);
});

test("a failure inside a for await loop closes the connection", async () => {
  const ended = fake([OK, OK, result(["id"], [["1"]])]);
  await assert.rejects(async () => {
    for await (const _ of await LiveSubscription.open("", "SELECT 1")) {
    }
  }, ProtocolError);
  assert.ok(ended.value);
});

test("the end of the stream closes the connection", async () => {
  const empty = result(COLUMNS, []);
  const ended = fake([OK, OK, result(COLUMNS, ROWS), empty, empty]);
  const events = [];
  for await (const event of await LiveSubscription.open("", "SELECT 1")) {
    events.push(event);
  }
  assert.equal(events.length, 2);
  assert.ok(ended.value);
});

test("overlapping reads take turns", async () => {
  const later = result(COLUMNS, [["7", "t", null, null]]);
  fake([OK, OK, result(COLUMNS, ROWS), later]);
  const live = await LiveSubscription.open("", "SELECT 1");
  const [first, second] = await Promise.all([live.next(), live.next()]);
  assert.equal(first.value.upper, 6n);
  assert.equal(second.value.upper, 7n);
});

test("results stay in text format and unpipelined", async () => {
  const clients = [];
  fake([OK, OK], clients);
  pg.defaults.binary = true;
  try {
    await LiveSubscription.open({ pipeline: true }, "SELECT 1");
  } finally {
    pg.defaults.binary = false;
  }
  assert.deepEqual(clients[0], { binary: false, pipeline: false });
});

test("a result of another command is an error", async () => {
  fake([OK, OK, { ...result(COLUMNS, []), command: "SELECT" }]);
  const live = await LiveSubscription.open("", "SELECT 1");
  await assert.rejects(
    live.next(),
    /expected FETCH, the server completed SELECT/,
  );
});

test("a host with several addresses reports each failure", async () => {
  fake([]);
  Client.prototype.connect.mock.mockImplementation(async () => {
    throw new AggregateError([new Error("a refused"), new Error("b refused")]);
  });
  await assert.rejects(LiveSubscription.open("", "SELECT 1"), {
    message: "a refused; b refused",
  });
});

test("a timestamp limit above the cap is refused", async () => {
  await assert.rejects(
    LiveSubscription.open("", "SELECT 1", { maxTimestampBytes: 2 ** 30 }),
    UsageError,
  );
});

test("an error the server sends between requests explains the next failure", async () => {
  fake([
    OK,
    OK,
    new Error("Client has encountered a connection error and is not queryable"),
  ]);
  let connected;
  Client.prototype.connect.mock.mockImplementation(async function () {
    connected = this;
  });
  const live = await LiveSubscription.open("", "SELECT 1");
  const expired = new pg.DatabaseError("authentication expired", 0, "error");
  expired.code = "28000";
  connected.emit("error", expired);
  await assert.rejects(live.next(), (error) => {
    assert.ok(error instanceof ServerError);
    assert.equal(error.sqlstate, "28000");
    assert.deepEqual(error.classification, {
      action: "kind",
      kind: "credentials_expired",
    });
    return true;
  });
});

test("a FETCH that releases more events than a call takes arguments", async () => {
  const many = 150_000;
  const rows = [["5", "t", null, null]];
  for (let i = 0; i < many; i++) rows.push(["5", "f", "1", String(i)]);
  rows.push(["6", "t", null, null]);
  fake([OK, OK, result(COLUMNS, rows)]);
  // Without a pending limit, every update is a snapshot chunk of its own.
  const live = await LiveSubscription.open("", "SELECT 1", {
    maxRows: many + 2,
    maxPendingBytes: 0,
    maxTimestampBytes: 2 ** 28,
  });
  const { value } = await live.next();
  assert.equal(value.event, "snapshot_chunk");
});

test("a closed subscription ends its iteration", async () => {
  fake([OK, OK, result(COLUMNS, ROWS)]);
  const live = await LiveSubscription.open("", "SELECT 1");
  await live.close();
  assert.deepEqual(await live.next(), { done: true, value: undefined });
});

test("invalid options fail before connecting", async () => {
  fake([]);
  for (const options of [
    { maxRows: 0 },
    { fetchTimeoutMs: -1 },
    { maxPendingBytes: 100, maxTimestampBytes: 100 },
  ]) {
    await assert.rejects(
      LiveSubscription.open("", "SELECT 1", options),
      UsageError,
    );
  }
  assert.equal(Client.prototype.connect.mock.callCount(), 0);
});
