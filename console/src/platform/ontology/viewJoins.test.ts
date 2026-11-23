// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { extractViewJoins } from "./viewJoins";

// Fixtures are `create_sql` as v26.50 stores it, for the auction demo:
// u2 accounts, u3 auctions, u4 bids, u5 organizations, u6 users,
// u7 winning_bids.

const pair = (
  leftId: string,
  leftColumn: string,
  rightId: string,
  rightColumn: string,
) => ({
  left: { objectId: leftId, column: leftColumn },
  right: { objectId: rightId, column: rightColumn },
});

describe("extractViewJoins", () => {
  it("reads aliased joins", () => {
    const sql = `CREATE VIEW "materialize"."agents"."org_spend" AS SELECT "o"."id" AS "org_id", "o"."name", "pg_catalog"."sum"("w"."amount") AS "spend" FROM [u5 AS "materialize"."auction"."organizations"] AS "o" JOIN [u6 AS "materialize"."auction"."users"] AS "u" ON "u"."org_id" = "o"."id" JOIN [u7 AS "materialize"."auction"."winning_bids"] AS "w" ON "w"."buyer" = "u"."id" GROUP BY "o"."id", "o"."name"`;
    expect(extractViewJoins(sql).pairs).toEqual([
      pair("u6", "org_id", "u5", "id"),
      pair("u7", "buyer", "u6", "id"),
    ]);
  });

  it("reads the columns a view passes through, by their output name", () => {
    const sql = `CREATE VIEW "materialize"."agents"."org_spend" AS SELECT "o"."id" AS "org_id", "o"."name", "pg_catalog"."sum"("w"."amount") AS "spend" FROM [u5 AS "materialize"."auction"."organizations"] AS "o" JOIN [u6 AS "materialize"."auction"."users"] AS "u" ON "u"."org_id" = "o"."id" JOIN [u7 AS "materialize"."auction"."winning_bids"] AS "w" ON "w"."buyer" = "u"."id" GROUP BY "o"."id", "o"."name"`;
    expect(extractViewJoins(sql).outputs).toEqual([
      { column: "org_id", source: { objectId: "u5", column: "id" } },
      { column: "name", source: { objectId: "u5", column: "name" } },
    ]);
  });

  it("reads passed-through columns after DISTINCT ON", () => {
    const sql = `CREATE MATERIALIZED VIEW "materialize"."auction"."winning_bids" IN CLUSTER [u2] WITH (REFRESH = ON COMMIT) AS SELECT DISTINCT ON ("a"."id") "b"."id" AS "bid_id", "a"."id" AS "auction_id", "b"."buyer" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN [u4 AS "materialize"."auction"."bids"] AS "b" ON "a"."id" = "b"."auction_id" AS OF 1794587001000`;
    expect(extractViewJoins(sql).outputs).toEqual([
      { column: "bid_id", source: { objectId: "u4", column: "id" } },
      { column: "auction_id", source: { objectId: "u3", column: "id" } },
      { column: "buyer", source: { objectId: "u4", column: "buyer" } },
    ]);
  });

  it("resolves unqualified output columns only over a single relation", () => {
    const single = `CREATE VIEW "v" AS SELECT "seller", "pg_catalog"."count"(*) AS "c" FROM [u3 AS "materialize"."auction"."auctions"] GROUP BY "seller"`;
    expect(extractViewJoins(single).outputs).toEqual([
      { column: "seller", source: { objectId: "u3", column: "seller" } },
    ]);
    const joined = `CREATE VIEW "v" AS SELECT "org_id" FROM [u6 AS "materialize"."auction"."users"] JOIN [u2 AS "materialize"."auction"."accounts"] USING ("org_id")`;
    expect(extractViewJoins(joined).outputs).toEqual([]);
  });

  it("reads no passed-through columns across a set operation", () => {
    const sql = `CREATE VIEW "v" AS SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" UNION ALL SELECT "b"."id" FROM [u4 AS "materialize"."auction"."bids"] AS "b"`;
    expect(extractViewJoins(sql).outputs).toEqual([]);
  });

  it("reads joins in materialized views, past their WHERE clause", () => {
    const sql = `CREATE MATERIALIZED VIEW "materialize"."auction"."winning_bids" IN CLUSTER [u2] WITH (REFRESH = ON COMMIT) AS SELECT DISTINCT ON ("a"."id") "b"."id" AS "bid_id", "a"."id" AS "auction_id", "b"."buyer", "a"."seller", "a"."item", "b"."amount", "a"."end_time" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN [u4 AS "materialize"."auction"."bids"] AS "b" ON "a"."id" = "b"."auction_id" WHERE "b"."bid_time" < "a"."end_time" AND "mz_catalog"."mz_now"() >= "a"."end_time" ORDER BY "a"."id", "b"."amount" DESC, "b"."bid_time", "b"."buyer" AS OF 1794587001000`;
    expect(extractViewJoins(sql).pairs).toEqual([
      pair("u3", "id", "u4", "auction_id"),
    ]);
  });

  it("resolves unaliased relations by their name", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_noalias" AS SELECT "bids"."id", "auctions"."item" FROM [u4 AS "materialize"."auction"."bids"] JOIN [u3 AS "materialize"."auction"."auctions"] ON "bids"."auction_id" = "auctions"."id"`;
    expect(extractViewJoins(sql).pairs).toEqual([
      pair("u4", "auction_id", "u3", "id"),
    ]);
  });

  it("reads semijoins and correlated conditions", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_sub" AS SELECT * FROM [u4 AS "materialize"."auction"."bids"] AS "b" WHERE "b"."auction_id" IN (SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" WHERE "a"."seller" = "b"."buyer")`;
    expect(extractViewJoins(sql).pairs).toEqual([
      pair("u4", "auction_id", "u3", "id"),
      pair("u3", "seller", "u4", "buyer"),
    ]);
  });

  it("does not resolve CTE names", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_cte" AS WITH "x" AS (SELECT "seller", "pg_catalog"."count"(*) AS "c" FROM [u3 AS "materialize"."auction"."auctions"] GROUP BY "seller") SELECT "u"."name", "x"."c" FROM "x", [u6 AS "materialize"."auction"."users"] AS "u" WHERE "u"."id" = "x"."seller"`;
    expect(extractViewJoins(sql).pairs).toEqual([]);
  });

  it("lists each relation read once, including inside CTEs", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_inputs" AS WITH "x" AS (SELECT "seller" FROM [u3 AS "materialize"."auction"."auctions"]) SELECT "u"."name" FROM "x", [u6 AS "materialize"."auction"."users"] AS "u" JOIN [u3 AS "materialize"."auction"."auctions"] AS "a" ON "a"."seller" = "u"."id"`;
    expect(extractViewJoins(sql).inputs).toEqual(["u3", "u6"]);
  });

  it("does not resolve an alias bound to two relations", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_shadow" AS SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN (SELECT "a"."id" FROM [u4 AS "materialize"."auction"."bids"] AS "a") AS "s" ON "s"."id" = "a"."id"`;
    expect(extractViewJoins(sql).pairs).toEqual([]);
  });

  it("skips conditions on expressions, and casts to types", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_expr" AS SELECT 1 FROM [u4 AS "materialize"."auction"."bids"] AS "b" JOIN [u3 AS "materialize"."auction"."auctions"] AS "a" ON "a"."id" + 0 = "b"."auction_id" AND "a"."seller"::[s46 AS "pg_catalog"."text"] = "b"."buyer"::[s46 AS "pg_catalog"."text"]`;
    expect(extractViewJoins(sql)).toEqual({
      inputs: ["u4", "u3"],
      pairs: [],
      usingJoins: [],
      outputs: [],
    });
  });

  it("reports USING joins with the relations joined before", () => {
    const sql = `CREATE VIEW "materialize"."auction"."t_using" AS SELECT "users"."name", "a"."balance" FROM [u6 AS "materialize"."auction"."users"] JOIN [u2 AS "materialize"."auction"."accounts"] AS "a" USING ("org_id")`;
    expect(extractViewJoins(sql).usingJoins).toEqual([
      { right: "u2", candidates: ["u6"], columns: ["org_id"] },
    ]);
  });

  it("does not resolve an alias that renames columns", () => {
    const sql = `CREATE VIEW "v" AS SELECT 1 FROM [u4 AS "materialize"."auction"."bids"] AS "b" ("x", "auction_id") JOIN [u3 AS "materialize"."auction"."auctions"] AS "a" ON "a"."id" = "b"."auction_id"`;
    expect(extractViewJoins(sql).pairs).toEqual([]);
  });

  it("keeps quotes and keywords inside strings out of the scan", () => {
    const sql = `CREATE VIEW "v" AS SELECT 'ON "a"."id" = "b"."x"' FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN [u4 AS "materialize"."auction"."bids"] AS "b" ON "a"."id" = "b"."auction_id" WHERE "b"."note" <> 'it''s'`;
    expect(extractViewJoins(sql).pairs).toEqual([
      pair("u3", "id", "u4", "auction_id"),
    ]);
  });
});
