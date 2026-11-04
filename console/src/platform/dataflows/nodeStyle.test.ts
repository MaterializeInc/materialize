// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hcl, rgb } from "d3";
import { describe, expect, it } from "vitest";

import { darkColors } from "~/theme/dark";
import { lightColors } from "~/theme/light";

import {
  formatCount,
  formatCountSi,
  formatSkew,
  hashString,
  heatColorFor,
  hexToRgba,
  nodeFillColor,
  operatorColor,
  operatorFamily,
  prettyPrintChannelType,
  relativeLuminance,
  statLines,
  textColorFor,
} from "./nodeStyle";

const OPERATOR_PALETTE = lightColors.dataflow.operator;
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

describe("operatorFamily", () => {
  it.each([
    ["Join", "join"],
    ["Join(Linear)", "join"],
    ["LinearJoinKeyPreparation", "join"],
    ["Reduce", "reduce"],
    ["ReduceAccumulable", "reduce"],
    ["Reduced TopK input", "reduce"],
    ["Distinct errors", "reduce"],
    ["Arrange bundle err", "arrange"],
    ['ArrangeBy[[Column(0, "id")]]', "arrange"],
    ["ArrangementSize", "arrange"],
    ["Arranged TopK input", "arrange"],
    ["Arrange ReduceMinsMaxes", "arrange"],
    ["FlatMap", "map"],
    ["CollectionFlatMap", "map"],
    ["ExplodeOne", "map"],
    ["Source(User(1))", "io"],
    ["shard_source_descs(u3)", "io"],
    ["persist_source::decode_and_mfp(u3)", "io"],
    ["Index(s2, [Column(0)])", "io"],
    ["mv_sink(u4)::write", "io"],
    ["SubscribeSink(Transient(1))", "io"],
    ["Concatenate", "other"],
    ["Probe", "other"],
  ])("%s is %s", (name, family) => {
    expect(operatorFamily(name)).toEqual(family);
  });
});

describe("operator palette tokens", () => {
  const themes = [
    ["light", lightColors],
    ["dark", darkColors],
  ] as const;

  it.each(themes)("%s: fills keep text at contrast >= 4.5", (_, theme) => {
    for (const fill of [
      ...Object.values(theme.dataflow.operator),
      theme.dataflow.lirGroup,
    ]) {
      // textColorFor returns 3-digit hex, which relativeLuminance can't parse.
      const text = rgb(textColorFor(fill)).formatHex();
      const [lighter, darker] = [fill, text]
        .map(relativeLuminance)
        .sort((a, b) => b - a);
      expect((lighter + 0.05) / (darker + 0.05)).toBeGreaterThanOrEqual(4.5);
    }
  });

  it.each(themes)(
    "%s: fills share one lightness and avoid the region hues",
    (_, theme) => {
      const lightness = Object.values(theme.dataflow.operator).map(
        (fill) => hcl(fill).l,
      );
      expect(Math.max(...lightness) - Math.min(...lightness)).toBeLessThan(2);
      // Regions use the purple and green accents, LIR groups reserve one of
      // them too, so operator hues must stay clear of both. Gray has no hue.
      const reserved = [theme.accent.purple, theme.accent.green].map(
        (c) => hcl(c).h,
      );
      for (const fill of Object.values(theme.dataflow.operator)) {
        const { c, h } = hcl(fill);
        if (c < 15) continue;
        for (const r of reserved) {
          const distance = Math.abs(((h - r + 540) % 360) - 180);
          expect(distance).toBeGreaterThan(40);
        }
      }
    },
  );

  it("colors by family, identically for every name in it", () => {
    expect(operatorColor("Join", lightColors.dataflow.operator)).toEqual(
      operatorColor("Join(Linear)", lightColors.dataflow.operator),
    );
    expect(operatorColor("Join", lightColors.dataflow.operator)).not.toEqual(
      operatorColor("Reduce", lightColors.dataflow.operator),
    );
  });
});

describe("nodeFillColor", () => {
  it("colors an operator by its name, not by whether it's arranged", () => {
    const unarranged = nodeFillColor(
      { ...baseNode, kind: "operator" },
      OPERATOR_PALETTE,
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
      OPERATOR_PALETTE,
      ACCENT,
    );
    expect(unarranged).toEqual(OPERATOR_PALETTE.reduce);
    expect(arranged).toEqual(OPERATOR_PALETTE.reduce);
  });

  it("still colors a region by whether it's arranged, unaffected by name", () => {
    const noArrangement = nodeFillColor(
      { ...baseNode, kind: "region" },
      OPERATOR_PALETTE,
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
      OPERATOR_PALETTE,
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

describe("formatCountSi", () => {
  it("keeps three significant digits under an SI prefix", () => {
    expect(formatCountSi(577351288n)).toEqual("577M");
    expect(formatCountSi(1214000000n)).toEqual("1.21G");
    expect(formatCountSi(12345n)).toEqual("12.3k");
    expect(formatCountSi(42n)).toEqual("42");
  });

  it("moves to the next prefix when rounding reaches 1000", () => {
    expect(formatCountSi(999n)).toEqual("999");
    expect(formatCountSi(999600n)).toEqual("1M");
    expect(formatCountSi(999400n)).toEqual("999k");
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
    expect(statLines(withStats)).toEqual(["3.0s · 221M r · 4 KB"]);
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
    expect(statLines(withStats)).toEqual(["3.0s"]);
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

describe("heatColorFor", () => {
  // WCAG contrast of the better of black and white text on a hex background.
  const bestContrast = (hex: string) => {
    const channel = (i: number) => {
      const c = parseInt(hex.slice(i, i + 2), 16) / 255;
      return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    };
    const l = 0.2126 * channel(1) + 0.7152 * channel(3) + 0.0722 * channel(5);
    return Math.max((l + 0.05) / 0.05, 1.05 / (l + 0.05));
  };

  it.each(["light", "dark"] as const)(
    "%s ramp keeps text readable on every step",
    (mode) => {
      const heat = heatColorFor(mode);
      for (let i = 0; i <= 20; i++) {
        const color = heat(i / 20);
        expect(color).toMatch(/^#[0-9a-f]{6}$/);
        expect(bestContrast(color)).toBeGreaterThanOrEqual(4.5);
      }
    },
  );

  it("differs per theme", () => {
    expect(heatColorFor("light")(1)).not.toEqual(heatColorFor("dark")(1));
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
