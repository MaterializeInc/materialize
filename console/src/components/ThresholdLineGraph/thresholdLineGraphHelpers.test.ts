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
  assignHighlightColors,
  generateRainbowPalette,
  isBreaching,
  quantizeThreshold,
  spreadIndices,
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

describe("generateRainbowPalette", () => {
  it("spreads hues evenly around the wheel", () => {
    expect(generateRainbowPalette(4)).toEqual([
      "hsl(0, 90%, 50%)",
      "hsl(90, 90%, 50%)",
      "hsl(180, 90%, 50%)",
      "hsl(270, 90%, 50%)",
    ]);
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
});

describe("spreadIndices", () => {
  it("visits every index exactly once", () => {
    for (const count of [1, 2, 3, 4, 5, 7, 12, 16, 46, 47, 100]) {
      const order = spreadIndices(count);
      expect(order).toHaveLength(count);
      expect(new Set(order).size).toBe(count);
      expect(Math.max(...order)).toBe(count - 1);
    }
  });

  it("returns nothing for an empty set", () => {
    expect(spreadIndices(0)).toEqual([]);
  });

  it("separates neighbouring ranks in a large set", () => {
    // The regression this exists for: sequential order put the four worst
    // breaches within 24 degrees of each other out of 360.
    const count = 46;
    const order = spreadIndices(count);
    const degrees = order.slice(0, 4).map((slot) => (slot * 360) / count);
    const gaps = degrees
      .slice(1)
      .map((d, i) => Math.abs(d - degrees[i]))
      .map((gap) => Math.min(gap, 360 - gap));
    for (const gap of gaps) {
      expect(gap).toBeGreaterThan(60);
    }
  });

  it("leaves a small set in its natural order", () => {
    // Three hues are already 120 degrees apart; striding would gain nothing.
    expect(spreadIndices(3)).toEqual([0, 1, 2]);
  });
});

describe("assignHighlightColors", () => {
  it("colors breaching lines worst first", () => {
    const colors = assignHighlightColors({
      lines: [line("low", 3), line("high", 9), line("mid", 5)],
      threshold: 2,
    });
    expect([...colors]).toEqual([
      ["high", "hsl(0, 90%, 50%)"],
      ["mid", "hsl(120, 90%, 50%)"],
      ["low", "hsl(240, 90%, 50%)"],
    ]);
  });

  it("leaves lines under the threshold uncolored", () => {
    const colors = assignHighlightColors({
      lines: [line("over", 3), line("under", 1), line("nothing", null)],
      threshold: 2,
    });
    expect([...colors.keys()]).toEqual(["over"]);
  });

  it("colors every breaching line however many there are", () => {
    // The regression this exists for: a fixed palette used to run out here and
    // leave the remainder looking like context.
    const lines = Array.from({ length: 46 }, (_unused, i) =>
      line(`k${String(i).padStart(2, "0")}`, i + 1),
    );
    const colors = assignHighlightColors({ lines, threshold: 0 });
    expect(colors.size).toBe(46);
    expect(new Set(colors.values()).size).toBe(46);
  });

  it("gives no two lines the same color", () => {
    const lines = Array.from({ length: 12 }, (_unused, i) => line(`k${i}`, 9));
    const colors = assignHighlightColors({ lines, threshold: 0 });
    expect(new Set(colors.values()).size).toBe(12);
  });

  it("does not put the two worst breaches in neighbouring hues", () => {
    const lines = Array.from({ length: 46 }, (_unused, i) =>
      line(`k${String(i).padStart(2, "0")}`, i + 1),
    );
    const colors = assignHighlightColors({ lines, threshold: 0 });
    const hueOf = (key: string) =>
      parseInt(/hsl\((\d+)/.exec(colors.get(key) ?? "")?.[1] ?? "", 10);

    const worst = hueOf("k45");
    const secondWorst = hueOf("k44");
    const gap = Math.abs(worst - secondWorst);
    expect(Math.min(gap, 360 - gap)).toBeGreaterThan(60);
  });

  it("adds a hand-picked line to the breaching set rather than replacing it", () => {
    const colors = assignHighlightColors({
      lines: [line("breaching", 9), line("picked", 1), line("ignored", 1)],
      threshold: 2,
      selectedKeys: new Set(["picked"]),
    });
    expect([...colors.keys()]).toEqual(["breaching", "picked"]);
  });

  it("does not spend two colors on a line that is both picked and breaching", () => {
    const colors = assignHighlightColors({
      lines: [line("both", 9)],
      threshold: 2,
      selectedKeys: new Set(["both"]),
    });
    expect([...colors.keys()]).toEqual(["both"]);
  });

  it("assigns the same colors to equal breach values on every call", () => {
    const lines = [line("b", 5), line("a", 5), line("c", 5)];
    const first = assignHighlightColors({ lines, threshold: 0 });
    const second = assignHighlightColors({
      lines: [...lines].reverse(),
      threshold: 0,
    });
    expect([...first]).toEqual([...second]);
  });

  it("passes saturation and lightness through to the palette", () => {
    const colors = assignHighlightColors({
      lines: [line("a", 9)],
      threshold: 0,
      saturation: 55,
      lightness: 70,
    });
    expect(colors.get("a")).toBe("hsl(0, 55%, 70%)");
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
