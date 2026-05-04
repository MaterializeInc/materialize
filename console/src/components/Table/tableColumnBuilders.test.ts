// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

import { sortingFunctions } from "./tableColumnBuilders";

// Helper to create a minimal Row-like object for testing sorting
const mockRow = (values: Record<string, unknown>) =>
  ({
    getValue: (columnId: string) => values[columnId],
  }) as unknown as Row<unknown>;

describe("sortingFunctions.nullsLast", () => {
  const sort = sortingFunctions.nullsLast;

  it("sorts strings alphabetically", () => {
    const a = mockRow({ col: "apple" });
    const b = mockRow({ col: "banana" });
    expect(sort(a, b, "col")).toBeLessThan(0);
  });

  it("sorts numeric-prefixed strings by number, not lexicographically", () => {
    // e.g. cluster sizes "25cc" should sort before "100cc"
    const a = mockRow({ col: "25cc" });
    const b = mockRow({ col: "100cc" });
    expect(sort(a, b, "col")).toBeLessThan(0);
  });

  it("sorts numbers correctly", () => {
    const a = mockRow({ col: 10 });
    const b = mockRow({ col: 5 });
    expect(sort(a, b, "col")).toBeGreaterThan(0);
  });

  it("pushes nullish values to the end", () => {
    expect(
      sort(mockRow({ col: "apple" }), mockRow({ col: null }), "col"),
    ).toBeLessThan(0);
    expect(
      sort(mockRow({ col: undefined }), mockRow({ col: "banana" }), "col"),
    ).toBeGreaterThan(0);
  });

  it("treats nullish values as equal to each other", () => {
    expect(sort(mockRow({ col: null }), mockRow({ col: null }), "col")).toBe(0);
    expect(
      sort(mockRow({ col: null }), mockRow({ col: undefined }), "col"),
    ).toBe(0);
  });
});
