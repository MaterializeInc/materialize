// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Column } from "~/api/materialize/types";

import { SubscribeDiffs } from "./subscribeDiffs";

const column = (name: string): Column => ({
  name,
  type_oid: 25,
  type_len: -1,
  type_mod: -1,
});

const COLUMNS = ["mz_timestamp", "mz_progressed", "mz_diff", "k", "v"].map(
  column,
);

const update = (ts: string, diff: number, k: string, v: string) => [
  ts,
  false,
  String(diff),
  k,
  v,
];
const progress = (ts: string) => [ts, true, null, null, null];

describe("SubscribeDiffs", () => {
  it("exposes only data columns", () => {
    const diffs = new SubscribeDiffs();
    diffs.setColumns(COLUMNS);
    expect(diffs.columns.map((c) => c.name)).toEqual(["k", "v"]);
  });

  it("holds updates back until their timestamp closes", () => {
    const diffs = new SubscribeDiffs();
    diffs.setColumns(COLUMNS);
    expect(diffs.push(update("1", 1, "a", "x"))).toBe(false);
    expect(diffs.rows(10)).toEqual({ rows: [], total: 0 });
    expect(diffs.push(progress("2"))).toBe(true);
    expect(diffs.frontier).toBe("2");
    expect(diffs.rows(10)).toEqual({ rows: [["a", "x"]], total: 1 });
  });

  it("applies retractions and multiplicities", () => {
    const diffs = new SubscribeDiffs();
    diffs.setColumns(COLUMNS);
    diffs.push(update("1", 2, "a", "x"));
    diffs.push(update("1", 1, "b", "y"));
    diffs.push(progress("2"));
    diffs.push(update("2", -1, "b", "y"));
    diffs.push(update("2", 1, "b", "z"));
    diffs.push(progress("3"));
    expect(diffs.rows(10)).toEqual({
      rows: [
        ["a", "x"],
        ["a", "x"],
        ["b", "z"],
      ],
      total: 3,
    });
    expect(diffs.rows(1)).toEqual({ rows: [["a", "x"]], total: 3 });
  });

  it("applies the last updates when a SUBSCRIBE completes without progress", () => {
    const diffs = new SubscribeDiffs();
    diffs.setColumns(COLUMNS);
    diffs.push(update("1", 1, "a", "x"));
    diffs.complete();
    expect(diffs.rows(10)).toEqual({ rows: [["a", "x"]], total: 1 });
  });

  it("keeps a changed row in place", () => {
    const diffs = new SubscribeDiffs();
    diffs.setColumns(COLUMNS);
    diffs.push(update("1", 1, "a", "1"));
    diffs.push(update("1", 1, "b", "1"));
    diffs.push(update("1", 1, "c", "1"));
    diffs.push(progress("2"));
    diffs.push(update("2", -1, "a", "1"));
    diffs.push(update("2", 1, "a", "2"));
    diffs.push(progress("3"));
    expect(diffs.rows(10).rows.map((r) => r[0])).toEqual(["a", "b", "c"]);
  });
});
