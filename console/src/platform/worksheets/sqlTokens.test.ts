// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  mapSqlCode,
  matchParen,
  splitSql,
  stripLeadingTrivia,
  trimStatement,
} from "./sqlTokens";

describe("splitSql", () => {
  it("separates code, comments and quoted text", () => {
    expect(
      splitSql("SELECT 'a''b', \"x\" -- c\n/* d /* e */ f */ $q$ g $q$ $1"),
    ).toEqual([
      { kind: "code", text: "SELECT " },
      { kind: "quoted", text: "'a''b'" },
      { kind: "code", text: ", " },
      { kind: "quoted", text: '"x"' },
      { kind: "code", text: " " },
      { kind: "comment", text: "-- c" },
      { kind: "code", text: "\n" },
      { kind: "comment", text: "/* d /* e */ f */" },
      { kind: "code", text: " " },
      { kind: "quoted", text: "$q$ g $q$" },
      { kind: "code", text: " $1" },
    ]);
  });

  it("honours backslash escapes in E strings only", () => {
    expect(splitSql("E'it\\'s' 'a\\'")).toEqual([
      { kind: "code", text: "E" },
      { kind: "quoted", text: "E'it\\'s'".slice(1) },
      { kind: "code", text: " " },
      { kind: "quoted", text: "'a\\'" },
    ]);
  });

  it("runs an unterminated quote to the end", () => {
    expect(splitSql("SELECT 'oops")).toEqual([
      { kind: "code", text: "SELECT " },
      { kind: "quoted", text: "'oops" },
    ]);
  });
});

describe("mapSqlCode", () => {
  it("leaves comments and strings untouched", () => {
    expect(mapSqlCode("a -- a\n'a' a", (c) => c.replace(/a/g, "b"))).toBe(
      "b -- a\n'a' b",
    );
  });
});

describe("trimStatement", () => {
  it.each([
    ["SELECT 1;", "SELECT 1"],
    ["SELECT 1 -- note", "SELECT 1"],
    ["SELECT 1; -- note\n  ", "SELECT 1"],
    ["SELECT ';' /* x */ ;", "SELECT ';'"],
  ])("trims %j", (sql, expected) => {
    expect(trimStatement(sql)).toBe(expected);
  });
});

describe("stripLeadingTrivia", () => {
  it("drops leading comments and whitespace", () => {
    expect(stripLeadingTrivia("-- a\n /* b */\n  SELECT 1")).toBe("SELECT 1");
  });
});

describe("matchParen", () => {
  it("ignores parentheses in comments and strings", () => {
    const sql = "(SELECT ')' -- )\n, 1) rest";
    expect(sql.slice(0, matchParen(sql))).toBe("(SELECT ')' -- )\n, 1)");
  });

  it("reports unbalanced input", () => {
    expect(matchParen("(SELECT (1)")).toBeUndefined();
  });
});
