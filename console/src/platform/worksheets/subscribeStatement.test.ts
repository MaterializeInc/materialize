// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  isQueryStatement,
  runsLive,
  toCellQuery,
  unwrapSubscribe,
  wrapInSubscribe,
} from "./subscribeStatement";

describe("toCellQuery", () => {
  it("passes non-SUBSCRIBE statements through", () => {
    expect(toCellQuery("SELECT 1;")).toEqual({
      query: "SELECT 1;",
      isSubscribe: false,
    });
  });

  it.each([
    ["SUBSCRIBE test", "SELECT * FROM test"],
    ["subscribe to test;", "SELECT * FROM test"],
    [
      'SUBSCRIBE TO materialize.public."My Table"',
      'SELECT * FROM materialize.public."My Table"',
    ],
    ["SUBSCRIBE {{base}}", "SELECT * FROM {{base}}"],
    [
      "SUBSCRIBE (SELECT a FROM t WHERE b = ')')",
      "SELECT a FROM t WHERE b = ')'",
    ],
    ["SUBSCRIBE TO select * from test;", "select * from test"],
  ])("unwraps %s", (source, query) => {
    expect(toCellQuery(source)).toEqual({ query, isSubscribe: true });
  });

  it.each([
    "SUBSCRIBE test WITH (SNAPSHOT = false)",
    "SUBSCRIBE (SELECT 1) ENVELOPE UPSERT (KEY (a))",
  ])("rejects options in %s", (source) => {
    expect(toCellQuery(source).error).toMatch(/options are not supported/);
  });

  it("allows comments around the statement and inside the query", () => {
    expect(
      toCellQuery("-- watch\nSUBSCRIBE (SELECT ')' -- (\n) -- done"),
    ).toEqual({ query: "SELECT ')' -- (\n", isSubscribe: true });
  });

  it("rejects unbalanced parentheses", () => {
    expect(toCellQuery("SUBSCRIBE (SELECT 1").error).toBe(
      "Unbalanced parentheses",
    );
  });
});

describe("runsLive", () => {
  it("runs SUBSCRIBE statements live", () => {
    expect(runsLive("-- watch it\nSUBSCRIBE test")).toBe(true);
  });

  it("runs everything else once", () => {
    expect(runsLive("SELECT 1")).toBe(false);
    expect(runsLive("insert into test values (4)")).toBe(false);
  });
});

describe("wrapInSubscribe and unwrapSubscribe", () => {
  it.each([
    ["SELECT 1", "SUBSCRIBE (SELECT 1)"],
    ["SELECT 1;", "SUBSCRIBE (SELECT 1);"],
    ["-- note\nSELECT 1 -- tail", "-- note\nSUBSCRIBE (SELECT 1) -- tail"],
    ["SELECT a\nFROM t", "SUBSCRIBE (\nSELECT a\nFROM t\n)"],
  ])("wraps %j and unwraps it back", (source, wrapped) => {
    expect(wrapInSubscribe(source)).toBe(wrapped);
    expect(unwrapSubscribe(wrapped)).toBe(source);
  });

  it("unwraps a SUBSCRIBE of an object into a query", () => {
    expect(unwrapSubscribe("SUBSCRIBE TO test")).toBe("SELECT * FROM test");
  });

  it("refuses to unwrap a SUBSCRIBE with options", () => {
    expect(
      unwrapSubscribe("SUBSCRIBE t WITH (SNAPSHOT = false)"),
    ).toBeUndefined();
  });

  it("only unwraps SUBSCRIBE statements", () => {
    expect(unwrapSubscribe("SELECT 1")).toBeUndefined();
  });
});

describe("isQueryStatement", () => {
  it.each([
    ["/* q */ WITH a AS (SELECT 1) SELECT * FROM a", true],
    ["(SELECT 1) UNION (SELECT 2)", true],
    ["insert into test values (4)", false],
    ["CREATE TABLE t (x int)", false],
  ])("classifies %j", (source, expected) => {
    expect(isQueryStatement(source)).toBe(expected);
  });
});
