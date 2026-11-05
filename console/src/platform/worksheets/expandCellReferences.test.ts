// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  expandCellReferences,
  findCellReferences,
  renameCellReferences,
  ResolvedReference,
} from "./expandCellReferences";

const resolver =
  (sources: Record<string, string>) =>
  (name: string): ResolvedReference =>
    name in sources
      ? { source: sources[name] }
      : { error: `Unknown cell reference: {{${name}}}` };

describe("expandCellReferences", () => {
  it("leaves SQL without references untouched apart from the trailing semicolon", () => {
    expect(expandCellReferences("SELECT 1;\n", resolver({}))).toEqual({
      sql: "SELECT 1",
      dependencies: [],
    });
  });

  it("expands references transitively into aliased subqueries", () => {
    const result = expandCellReferences(
      "SELECT * FROM {{doubled}} JOIN {{base}} ON true",
      resolver({
        base: "SELECT 1 AS x; -- trailing comment",
        doubled: "SELECT x * 2 AS y FROM {{ base }}",
      }),
    );
    expect(result.sql).toBe(
      "SELECT * FROM (\n" +
        "SELECT x * 2 AS y FROM (\nSELECT 1 AS x\n) AS base\n" +
        ") AS doubled JOIN (\nSELECT 1 AS x\n) AS base ON true",
    );
    expect(result.dependencies.sort()).toEqual(["base", "doubled"]);
  });

  it("ignores references in comments and string literals", () => {
    const sql = "SELECT '{{nope}}' -- see {{nope}}\n, 1";
    expect(expandCellReferences(sql, resolver({})).sql).toBe(sql);
  });

  it("reports the resolver's error", () => {
    expect(
      expandCellReferences("SELECT * FROM {{nope}}", resolver({})).error,
    ).toBe("Unknown cell reference: {{nope}}");
  });

  it("reports cycles", () => {
    expect(
      expandCellReferences(
        "SELECT * FROM {{a}}",
        resolver({ a: "SELECT * FROM {{b}}", b: "SELECT * FROM {{a}}" }),
      ).error,
    ).toBe("Cell reference cycle: a → b → a");
  });
});

describe("findCellReferences", () => {
  it("lists references outside comments and quotes", () => {
    expect(
      findCellReferences(
        "SELECT * FROM {{a}} JOIN {{ b }} -- {{c}}\n, '{{d}}'",
      ),
    ).toEqual(["a", "b"]);
  });
});

describe("renameCellReferences", () => {
  it("renames only the given cell, outside comments and quotes", () => {
    expect(
      renameCellReferences(
        "SELECT * FROM {{ a }} JOIN {{ab}} -- {{a}}",
        "a",
        "z",
      ),
    ).toBe("SELECT * FROM {{z}} JOIN {{ab}} -- {{a}}");
  });
});
