// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hcl } from "d3";
import { describe, expect, it } from "vitest";

import {
  assignLineColors,
  generateRainbowPalette,
  isBreaching,
  quantizeThreshold,
} from "./thresholdLineGraphHelpers";
import { ThresholdLineSeries } from "./types";

/** A line judged by `breachValue`; the series itself is irrelevant here. */
function line(
  key: string,
  breachValue: number | null,
): ThresholdLineSeries<never> {
  return { key, yAccessor: () => null, breachValue };
}

describe("isBreaching", () => {
  it("treats the threshold as an inclusive floor", () => {
    // A point exactly at the threshold is drawn touching the shaded band, so
    // counting it as healthy would contradict the picture.
    expect(isBreaching(line("a", 2.0), 2.0)).toBe(true);
    expect(isBreaching(line("a", 1.9), 2.0)).toBe(false);
  });

  it("never breaches without a value to judge", () => {
    expect(isBreaching(line("a", null), 0)).toBe(false);
  });
});

/** The hue a color actually renders at, in degrees. */
function hue(color: string) {
  return hcl(color).h;
}

/** The closest any two of these colors sit on the wheel, in degrees. */
function minHueGap(colors: string[]) {
  const hues = colors.map(hue).sort((a, b) => a - b);
  return Math.min(
    ...hues.map((h, i) => {
      const next = hues[(i + 1) % hues.length];
      return (next - h + 360) % 360;
    }),
  );
}

describe("generateRainbowPalette", () => {
  it("puts the first four colors about a quarter turn apart", () => {
    expect(minHueGap(generateRainbowPalette(4))).toBeGreaterThan(80);
  });

  it("narrows the spacing as the palette grows, without bunching", () => {
    // The sequence halves its spacing each time it doubles. Asserted with
    // tolerance because a color outside sRGB is clamped, which moves its hue.
    expect(minHueGap(generateRainbowPalette(8))).toBeGreaterThan(35);
    expect(minHueGap(generateRainbowPalette(16))).toBeGreaterThan(15);
    expect(minHueGap(generateRainbowPalette(32))).toBeGreaterThan(5);
  });

  it("never recolors an earlier entry when the palette grows", () => {
    // A line's color must not depend on how many other lines exist.
    expect(generateRainbowPalette(64).slice(0, 5)).toEqual(
      generateRainbowPalette(5),
    );
  });

  it("honors chroma and lightness overrides", () => {
    expect(generateRainbowPalette(2, 40, 70)).toEqual([
      hcl(0, 40, 70).formatHex(),
      hcl(180, 40, 70).formatHex(),
    ]);
  });

  it("returns nothing for an empty set", () => {
    expect(generateRainbowPalette(0)).toEqual([]);
  });

  it("gives distinct colors at the sizes a cluster reaches", () => {
    expect(new Set(generateRainbowPalette(128)).size).toBe(128);
  });
});

describe("assignLineColors", () => {
  it("colors every line, breached or not", () => {
    const colors = assignLineColors([
      line("over", 9),
      line("under", 1),
      line("nothing", null),
    ]);
    expect([...colors.keys()].sort()).toEqual(["nothing", "over", "under"]);
  });

  it("ranks the worst breach first", () => {
    const colors = assignLineColors([
      line("low", 3),
      line("high", 9),
      line("mid", 5),
    ]);
    expect([...colors.keys()]).toEqual(["high", "mid", "low"]);
    expect([...colors.values()]).toEqual(generateRainbowPalette(3));
  });

  it("does not depend on the order the lines arrive in", () => {
    // The regression this exists for: colors used to come partly from input
    // order, so re-sorting a table reshuffled them.
    const lines = [line("a", 5), line("b", 5), line("c", 3), line("d", null)];
    const forward = assignLineColors(lines);
    const backward = assignLineColors([...lines].reverse());
    expect([...forward].sort()).toEqual([...backward].sort());
  });

  it("gives no two lines the same color, below the wheel's ceiling", () => {
    const lines = Array.from({ length: 12 }, (_unused, i) => line(`k${i}`, i));
    expect(new Set(assignLineColors(lines).values()).size).toBe(12);
  });

  it("separates the worst breaches, which are the ones a threshold picks", () => {
    // Any threshold highlights a prefix of the ranking, so the prefix is what
    // has to be spread.
    const lines = Array.from({ length: 47 }, (_unused, i) =>
      line(`k${String(i).padStart(2, "0")}`, 100 - i),
    );
    const colors = assignLineColors(lines);
    const worstFour = ["k00", "k01", "k02", "k03"].map(
      (key) => colors.get(key) ?? "",
    );
    expect(minHueGap(worstFour)).toBeGreaterThan(80);
  });
});

describe("quantizeThreshold", () => {
  const range = { step: 0.1, min: 0, max: 10 };

  it("snaps to the step", () => {
    expect(quantizeThreshold(2.04, range)).toBe(2);
    expect(quantizeThreshold(2.06, range)).toBe(2.1);
  });

  it("does not leak floating point error into the value", () => {
    // 0.3 / 0.1 * 0.1 is 0.30000000000000004 without the decimal rounding, and
    // this value is rendered as a label.
    expect(quantizeThreshold(0.3, range)).toBe(0.3);
    expect(quantizeThreshold(0.7, range)).toBe(0.7);
  });

  it("clamps to the range", () => {
    expect(quantizeThreshold(-5, range)).toBe(0);
    expect(quantizeThreshold(99, range)).toBe(10);
  });

  it("keeps a whole-number step whole", () => {
    expect(quantizeThreshold(7.4, { step: 1, min: 0, max: 10 })).toBe(7);
  });

  it("handles a step written in exponent form", () => {
    expect(quantizeThreshold(0.00000123, { step: 1e-7, min: 0, max: 1 })).toBe(
      0.0000012,
    );
  });
});
