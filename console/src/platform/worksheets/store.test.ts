// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addToHistory, normalizeWorksheet } from "./store";

describe("normalizeWorksheet", () => {
  it.each([null, 42, "x", {}, { cells: "nope" }])(
    "falls back to the starter worksheet for %j",
    (value) => {
      const worksheet = normalizeWorksheet(value);
      expect(worksheet.cells.length).toBeGreaterThan(0);
      expect(worksheet.cluster).toBe("");
    },
  );

  it("fills in fields missing from older saves", () => {
    const worksheet = normalizeWorksheet({
      cells: [
        { id: "a", name: "a", source: "SELECT 1", live: true },
        { id: "b", name: "b", kind: "chart" },
        "garbage",
      ],
      columns: 3,
    });
    expect(worksheet).toEqual({
      cluster: "",
      columns: 1,
      cells: [
        {
          id: "a",
          name: "a",
          // The old live flag becomes a SUBSCRIBE in the SQL.
          source: "SUBSCRIBE (SELECT 1)",
          live: false,
          collapsed: false,
          kind: "sql",
        },
        {
          id: "b",
          name: "b",
          source: "",
          live: false,
          collapsed: false,
          kind: "chart",
          chart: { source: "", mark: "bar", x: "", y: "", color: "" },
        },
      ],
    });
  });

  it("replaces retired starter comments, which become dashboard titles", () => {
    const sources = normalizeWorksheet({
      cells: [
        {
          source:
            "-- Run a cell with ▶ or Shift-Enter. Other cells can use it by name.\nSELECT 1",
        },
        { source: "-- Run a cell with ▶ or Shift-Enter. Edited.\nSELECT 2" },
      ],
    }).cells.map((c) => c.source);
    expect(sources).toEqual([
      "-- Replicas of every cluster\nSELECT 1",
      "-- Run a cell with ▶ or Shift-Enter. Edited.\nSELECT 2",
    ]);
  });
});

describe("addToHistory", () => {
  it("puts the newest entry first and drops its earlier copy", () => {
    expect(addToHistory(["b", "a"], " a\n")).toEqual(["a", "b"]);
  });

  it("ignores blank queries", () => {
    expect(addToHistory(["a"], "  ")).toEqual(["a"]);
  });
});
