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

// Live subscriptions against a running Materialize at MATERIALIZE_URL.

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const { after, before, describe, test } = require("node:test");
const { Client } = require("pg");

const { LiveSubscription, ServerError } = require("..");

const URL = process.env.MATERIALIZE_URL;

describe("live", { skip: URL === undefined && "needs MATERIALIZE_URL" }, () => {
  const name = `node_live_${crypto.randomBytes(4).toString("hex")}`;
  const admin = new Client(URL);

  before(async () => {
    await admin.connect();
    await admin.query(`CREATE TABLE ${name} (id int)`);
    await admin.query(`INSERT INTO ${name} VALUES (1), (2)`);
  });

  after(async () => {
    await admin.query(`DROP TABLE ${name}`);
    await admin.end();
  });

  const ids = (event) => event.updates.map((update) => update.row[0]).sort();

  test("a table streams its snapshot, then its changes", async () => {
    const live = await LiveSubscription.open(URL, `SELECT * FROM ${name}`);
    try {
      const { value: snapshot } = await live.next();
      assert.equal(snapshot.event, "batch");
      assert.ok(snapshot.snapshot);
      assert.deepEqual(ids(snapshot), ["1", "2"]);
      assert.equal(typeof snapshot.updates[0].timestamp, "bigint");
      assert.equal(typeof live.asOf, "bigint");

      await admin.query(`INSERT INTO ${name} VALUES (3)`);
      for await (const event of live) {
        if (event.updates.length > 0) {
          assert.deepEqual(ids(event), ["3"]);
          assert.equal(typeof event.lower, "bigint");
          break;
        }
      }
    } finally {
      await live.close().catch(() => {});
    }
  });

  test("a missing table fails with its classification", async () => {
    await assert.rejects(
      LiveSubscription.open(URL, "SELECT * FROM no_such_table_here"),
      (error) => {
        assert.ok(error instanceof ServerError);
        assert.equal(error.sqlstate, "XX000");
        assert.match(error.message, /unknown catalog item/);
        assert.deepEqual(error.classification, {
          action: "check_object",
          otherwise: "fatal",
        });
        return true;
      },
    );
  });

  test("a query cannot smuggle in a second statement", async () => {
    // Each statement parses, so only the protocol refuses the text.
    await assert.rejects(
      LiveSubscription.open(URL, "SELECT 1); SELECT 2; SUBSCRIBE (SELECT 3"),
      (error) =>
        error instanceof ServerError && /multiple commands/.test(error.message),
    );
  });
});

test("a refused connection fails with its classification", async () => {
  await assert.rejects(
    LiveSubscription.open(
      "postgres://materialize@127.0.0.1:1/materialize",
      "SELECT 1",
    ),
    (error) => {
      assert.ok(error instanceof ServerError);
      assert.equal(error.sqlstate, undefined);
      assert.deepEqual(error.classification, {
        action: "kind",
        kind: "transient",
      });
      return true;
    },
  );
});
