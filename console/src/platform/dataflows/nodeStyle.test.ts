// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  formatCount,
  formatSkew,
  hashString,
  hexToRgba,
  lirGroupColor,
  nodeFillColor,
  operatorColor,
  prettyPrintChannelType,
  statLines,
  textColorFor,
} from "./nodeStyle";

const PALETTE = ["#111111", "#222222", "#333333", "#444444"];
const ACCENT = { arranged: "#aaaaaa", notArranged: "#bbbbbb" };

const baseNode = {
  id: "1",
  label: "Reduce",
  stats: null,
  transitive: null,
  own: null,
  ownSkew: null,
  transitiveSkew: null,
  overheadNs: null,
  childCount: 0,
  lir: [],
  address: null,
  operatorId: null,
  peers: [],
  direction: null,
};

describe("hashString", () => {
  it("is deterministic for the same string", () => {
    expect(hashString("abc")).toEqual(hashString("abc"));
  });

  it("differs for different strings often enough to be useful", () => {
    const hashes = new Set(["1", "2", "3", "4", "5", "6"].map(hashString));
    expect(hashes.size).toBeGreaterThan(1);
  });
});

describe("lirGroupColor", () => {
  it("is deterministic for the same lirId", () => {
    expect(lirGroupColor("42", PALETTE)).toEqual(lirGroupColor("42", PALETTE));
  });

  it("differs for different lirIds often enough to be useful", () => {
    const picked = new Set(
      ["1", "2", "3", "4", "5", "6"].map((id) => lirGroupColor(id, PALETTE)),
    );
    expect(picked.size).toBeGreaterThan(1);
  });

  it("only ever picks from the given palette", () => {
    expect(PALETTE).toContain(lirGroupColor("some-lir-id", PALETTE));
  });
});

describe("operatorColor", () => {
  it("is deterministic for the same operator name", () => {
    expect(operatorColor("Reduce", PALETTE)).toEqual(
      operatorColor("Reduce", PALETTE),
    );
  });

  it("differs for different operator names often enough to be useful", () => {
    const picked = new Set(
      ["Map", "Filter", "Reduce", "Join", "FlatMap", "Arrange"].map((name) =>
        operatorColor(name, PALETTE),
      ),
    );
    expect(picked.size).toBeGreaterThan(1);
  });

  it("only ever picks from the given palette", () => {
    expect(PALETTE).toContain(operatorColor("Reduce", PALETTE));
  });
});

describe("nodeFillColor", () => {
  it("colors an operator by its name, not by whether it's arranged", () => {
    const unarranged = nodeFillColor(
      { ...baseNode, kind: "operator" },
      PALETTE,
      ACCENT,
    );
    const arranged = nodeFillColor(
      {
        ...baseNode,
        kind: "operator",
        transitive: {
          arrangementRecords: 100n,
          arrangementSize: 100n,
          elapsedNs: 0n,
          scheduleCount: 0n,
        },
      },
      PALETTE,
      ACCENT,
    );
    expect(unarranged).toEqual(operatorColor("Reduce", PALETTE));
    expect(arranged).toEqual(operatorColor("Reduce", PALETTE));
  });

  it("still colors a region by whether it's arranged, unaffected by name", () => {
    const noArrangement = nodeFillColor(
      { ...baseNode, kind: "region" },
      PALETTE,
      ACCENT,
    );
    const arranged = nodeFillColor(
      {
        ...baseNode,
        kind: "region",
        transitive: {
          arrangementRecords: 100n,
          arrangementSize: 100n,
          elapsedNs: 0n,
          scheduleCount: 0n,
        },
      },
      PALETTE,
      ACCENT,
    );
    expect(noArrangement).toEqual(ACCENT.notArranged);
    expect(arranged).toEqual(ACCENT.arranged);
  });
});

describe("formatSkew", () => {
  it("formats a real ratio to 2 decimal places", () => {
    expect(formatSkew(1.5)).toEqual("1.50");
    expect(formatSkew(1.0244)).toEqual("1.02");
  });

  it("labels the no-data sentinel (0) distinctly from a real ratio", () => {
    expect(formatSkew(0)).toEqual("no data");
  });
});

describe("formatCount", () => {
  it("groups digits so a large count reads at a glance", () => {
    expect(formatCount(221245721n)).toEqual("221,245,721");
  });

  it("leaves a small count unchanged", () => {
    expect(formatCount(42n)).toEqual("42");
  });
});

describe("statLines", () => {
  const node = { ...baseNode, kind: "operator" as const };

  it("returns nothing when the node has no stats", () => {
    expect(statLines(node)).toEqual([]);
  });

  it("merges duration, records, and size onto one line, in that order", () => {
    const withStats = {
      ...node,
      stats: {
        arrangementRecords: 221245721n,
        arrangementSize: 4096n,
        elapsedNs: 3_000_000_000n,
        scheduleCount: 0n,
      },
    };
    expect(statLines(withStats)).toEqual(["3s · 221,245,721 r · 4 KB"]);
  });

  it("omits records and size when there are none, keeping duration alone", () => {
    const withStats = {
      ...node,
      stats: {
        arrangementRecords: 0n,
        arrangementSize: 0n,
        elapsedNs: 3_000_000_000n,
        scheduleCount: 0n,
      },
    };
    expect(statLines(withStats)).toEqual(["3s"]);
  });

  it("omits duration when there is none, keeping records and size", () => {
    const withStats = {
      ...node,
      stats: {
        arrangementRecords: 100n,
        arrangementSize: 1024n,
        elapsedNs: 0n,
        scheduleCount: 0n,
      },
    };
    expect(statLines(withStats)).toEqual(["100 r · 1 KB"]);
  });
});

describe("hexToRgba", () => {
  it("converts a hex color to rgba with the given alpha", () => {
    expect(hexToRgba("#391D7E", 0.15)).toEqual("rgba(57, 29, 126, 0.15)");
  });
});

describe("textColorFor", () => {
  it("picks light text on a dark background", () => {
    // colors.lineGraph's purple[700], the color from the reported contrast bug
    expect(textColorFor("#391D7E")).toEqual("#FFF");
  });

  it("picks dark text on a light background", () => {
    // colors.lineGraph's purple[200]
    expect(textColorFor("#C8B5FF")).toEqual("#111");
  });
});

describe("prettyPrintChannelType", () => {
  it("strips module paths, aliases Diff/Error, and brackets Vec", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<(mz_repr::row::Row, mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>",
      ),
    ).toEqual("[(Row, Timestamp, Diff)]");
  });

  it("handles the error-channel variant", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<(mz_compute::render::errors::DataflowErrorSer, mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>",
      ),
    ).toEqual("[(Error, Timestamp, Diff)]");
  });

  it("unwraps nested Vec and unrecognized generics without mangling them", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<alloc::rc::Rc<differential_dataflow::trace::implementations::ord_neu::val_batch::OrdValBatch<mz_row_spine::spines::RowRowLayout<((mz_repr::row::Row, mz_repr::row::Row), mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>>>>",
      ),
    ).toEqual("[Rc<OrdValBatch<RowRowLayout<((Row, Row), Timestamp, Diff)>>>]");
  });
});
