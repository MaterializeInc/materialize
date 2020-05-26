// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Client } from "pg";

const client = new Client({
  port: parseInt(process.env.PGPORT, 10) || 6875,
  database: process.env.PGDATABASE || "materialize",
});

beforeAll(async () => await client.connect());
afterAll(async () => await client.end());

describe("query api", () => {
  const bindError = ({ expected, actual, name = "" }) =>
    expect.objectContaining({
      code: "08P01",
      message:
        `bind message supplies ${actual} parameters, ` +
        `but prepared statement "${name}" requires ${expected}`,
    });

  it("should reject invalid queries with too few parameters", async () => {
    await expect(client.query("SELECT $1 || $2", ["1"])).rejects.toThrow(
      bindError({ expected: 2, actual: 1 }),
    );
  });

  it("should reject invalid queries with too many parameters", async () => {
    await expect(client.query("SELECT 1", ["1", "2"])).rejects.toThrow(
      bindError({ expected: 0, actual: 2 }),
    );
  });

  it("should include the prepared statement name in the error message", async () => {
    await expect(
      client.query({ text: "SELECT $1", name: "foo" }),
    ).rejects.toThrow(bindError({ expected: 1, actual: 0, name: "foo" }));
  });

  it("should allow queries with the correct number of parameters", async () => {
    const res = await client.query({
      text: "SELECT $1 || $2",
      values: ["1", "2"],
      rowMode: "array",
    });
    expect(res.rows).toEqual([["12"]]);
  });

  describe("list parameters", () => {
    it("should handle a simple int array", async () => {
      const res = await client.query({
        text: "SELECT $1::int list",
        values: ["{  1, NULL,   2}"],
        rowMode: "array",
      });
      expect(res.rows).toEqual([["{1,NULL,2}"]]);
    });

    it("should handle a nested text array", async () => {
      const res = await client.query({
        text: "SELECT $1::text list list",
        values: [`{ {  }, "{}", {a, "", "\\""}, "{a,\\"\\",\\"\\\\\\"\\"}"}`],
        rowMode: "array",
      });
      expect(res.rows).toEqual([[`{{},{},{a,"","\\""},{a,"","\\""}}`]]);
    });

    it("should reject mismatched types", async () => {
      await expect(
        client.query({
          text: "SELECT $1::int list",
          values: [`{a}`],
        }),
      ).rejects.toThrow(
        expect.objectContaining({
          code: "22023",
          message: expect.stringMatching(
            /unable to decode parameter:.*invalid digit found in string/,
          ),
        }),
      );
    });
  });
});
