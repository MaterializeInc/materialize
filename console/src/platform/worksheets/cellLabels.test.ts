// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { cellTitle, chartTitle, COUNT_OF_ROWS } from "./cellLabels";

describe("cellTitle", () => {
  it("uses the first sentence of a leading comment", () => {
    expect(
      cellTitle({
        name: "verdict",
        source: "-- Verdict: why it is behind. Details below.\nSELECT 1",
      }),
    ).toEqual({
      title: "Verdict: why it is behind",
      detail: "Verdict: why it is behind. Details below.",
    });
  });

  it("falls back to the cell name", () => {
    expect(cellTitle({ name: "totals", source: "SELECT 1" })).toEqual({
      title: "totals",
    });
  });
});

describe("chartTitle", () => {
  const base = { source: "s", mark: "bar" as const, color: "" };

  it("describes what is plotted", () => {
    expect(chartTitle({ ...base, x: "day", y: "total", color: "region" })).toBe(
      "total by day, per region",
    );
    expect(
      chartTitle({ ...base, mark: "pie", x: "type", y: COUNT_OF_ROWS }),
    ).toBe("Rows per type");
  });

  it("falls back to the source until axes are chosen", () => {
    expect(chartTitle({ ...base, x: "", y: "" })).toBe("s");
  });
});
