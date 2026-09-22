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
  return { key, label: key, yAccessor: () => null, breachValue };
}

describe("isBreaching", () => {
  it("treats the threshold as an exclusive floor", () => {
    expect(isBreaching(line("a", 2.0), 2.0)).toBe(false);
    expect(isBreaching(line("a", 2.1), 2.0)).toBe(true);
  });

  it("never breaches without a value to judge", () => {
    expect(isBreaching(line("a", null), 0)).toBe(false);
  });
});

/** The integer hue out of an `hsl(...)` string. */
function hue(color: string) {
  return parseInt(/hsl\((\d+)/.exec(color)?.[1] ?? "", 10);
}

describe("generateRainbowPalette", () => {
  it("puts the first four hues a quarter turn apart", () => {
    expect(
      generateRainbowPalette(4)
        .map(hue)
        .sort((a, b) => a - b),
    ).toEqual([0, 90, 180, 270]);
  });

  it("halves the spacing as the palette grows", () => {
    const spacing = (count: number) => {
      const hues = generateRainbowPalette(count)
        .map(hue)
        .sort((a, b) => a - b);
      return Math.min(...hues.slice(1).map((h, i) => h - hues[i]));
    };
    expect(spacing(2)).toBe(180);
    expect(spacing(4)).toBe(90);
    expect(spacing(8)).toBe(45);
    // 22.5 degrees, which alternates 23 and 22 on a whole-degree grid.
    expect(spacing(16)).toBe(22);
  });

  it("orders hues furthest-apart first", () => {
    expect(generateRainbowPalette(8).map(hue)).toEqual([
      0, 180, 90, 270, 45, 225, 135, 315,
    ]);
  });

  it("never recolors an earlier entry when the palette grows", () => {
    // A line's color must not depend on how many other lines exist.
    expect(generateRainbowPalette(64).slice(0, 5)).toEqual(
      generateRainbowPalette(5),
    );
  });

  it("honors saturation and lightness overrides", () => {
    expect(generateRainbowPalette(2, 40, 65)).toEqual([
      "hsl(0, 40%, 65%)",
      "hsl(180, 40%, 65%)",
    ]);
  });

  it("returns nothing for an empty set", () => {
    expect(generateRainbowPalette(0)).toEqual([]);
  });

  it("stays distinct up to a whole wheel of hues, then repeats", () => {
    expect(new Set(generateRainbowPalette(256)).size).toBe(256);
    expect(new Set(generateRainbowPalette(2048)).size).toBeLessThan(2048);
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
    expect([...colors]).toEqual([
      ["high", "hsl(0, 90%, 50%)"],
      ["mid", "hsl(180, 90%, 50%)"],
      ["low", "hsl(90, 90%, 50%)"],
    ]);
  });

  it("does not depend on the order the lines arrive in", () => {
    // The regression this exists for: colors used to come partly from input
    // order, so re-sorting a table reshuffled them.
    const lines = [line("a", 5), line("b", 5), line("c", 3), line("d", null)];
    const forward = assignLineColors(lines);
    const backward = assignLineColors([...lines].reverse());
    expect([...forward].sort()).toEqual([...backward].sort());
  });

  it("takes no threshold, so no threshold can recolor a line", () => {
    // Guards the property by signature: there is nothing to pass.
    expect(assignLineColors.length).toBe(1);
  });

  it("gives no two lines the same color, below the wheel's ceiling", () => {
    const lines = Array.from({ length: 12 }, (_unused, i) => line(`k${i}`, i));
    expect(new Set(assignLineColors(lines).values()).size).toBe(12);
  });

  it("separates the worst breaches, which are the ones a threshold picks", () => {
    // Any threshold highlights a prefix of the ranking, so the prefix is what
    // has to be spread. The first four land a quarter turn apart.
    const lines = Array.from({ length: 47 }, (_unused, i) =>
      line(`k${String(i).padStart(2, "0")}`, 100 - i),
    );
    const colors = assignLineColors(lines);
    const worstFour = ["k00", "k01", "k02", "k03"].map((k) =>
      hue(colors.get(k) ?? ""),
    );
    expect([...worstFour].sort((a, b) => a - b)).toEqual([0, 90, 180, 270]);
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
