// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ThresholdLineSeries } from "./types";

const DEFAULT_SATURATION = 90;
const DEFAULT_LIGHTNESS = 50;

/**
 * The `index`th hue of a sequence that repeatedly halves its own spacing: the
 * first two hues are 180 degrees apart, the first four 90, the first eight 45,
 * and so on. Every prefix is as evenly spread around the wheel as that many
 * hues can be.
 *
 * This is the bit-reversal (van der Corput) sequence scaled to 360 degrees.
 * Reversing the bits of the index turns counting up, which bunches values
 * together, into a walk that always bisects the largest remaining gap.
 *
 *   index   0     1     2     3     4     5     6     7
 *   hue     0   180    90   270    45   225   135   315
 *
 * The count is deliberately not an input. A line's hue depends only on its own
 * position, so adding or removing lines never recolors the ones already there,
 * and the color beside an object in a table keeps its meaning as the set it
 * belongs to changes.
 */
function hueAtIndex(index: number): number {
  let remaining = index;
  let denominator = 1;
  let fraction = 0;
  while (remaining > 0) {
    denominator *= 2;
    fraction += (remaining % 2) / denominator;
    remaining = Math.floor(remaining / 2);
  }
  return Math.round(fraction * 360);
}

/**
 * Hues spread around the color wheel, at a fixed saturation and lightness.
 *
 * Generated on demand rather than drawn from a fixed list. A threshold can put
 * any number of lines in play at once, and a fixed palette answers that badly
 * in both directions: it runs out, or it forces distinct lines to share a
 * color.
 *
 * The result is ordered so that the earliest colors are the furthest apart,
 * which is what suits a caller handing them out by rank. See `hueAtIndex`.
 *
 * Hues are whole degrees, so the first 360 colors are unique and beyond that
 * they repeat. Fractional hues would avoid the repeat on paper and gain
 * nothing in practice: a fraction of a degree is not a difference an eye can
 * resolve, so past 360 lines the colors stop distinguishing them either way.
 *
 * @param numColors How many colors to generate.
 * @param saturation Saturation percentage, 0 to 100.
 * @param lightness Lightness percentage, 0 to 100.
 */
export function generateRainbowPalette(
  numColors: number,
  saturation: number = DEFAULT_SATURATION,
  lightness: number = DEFAULT_LIGHTNESS,
): string[] {
  return Array.from(
    { length: numColors },
    (_unused, index) =>
      `hsl(${hueAtIndex(index)}, ${saturation}%, ${lightness}%)`,
  );
}

/** Whether the line's judged value sits strictly above the threshold. */
export function isBreaching<Datum>(
  line: ThresholdLineSeries<Datum>,
  threshold: number,
): boolean {
  return line.breachValue !== null && line.breachValue > threshold;
}

/**
 * A color for every line, keyed by series key.
 *
 * Assigned from `breachValue` alone, so a line keeps its color as the
 * threshold moves. The threshold decides whether a line is drawn in its color
 * or in grey, never which color it gets: recoloring on every drag tick would
 * make the swatch beside an object mean nothing from one moment to the next.
 *
 * Ranked worst first and handed out in palette order. The lines a threshold
 * highlights are always the top of that ranking, and the palette's earliest
 * colors are its most widely separated, so whatever the threshold, the
 * highlighted lines are the furthest apart in hue.
 *
 * Deterministic, ties breaking on key, so the same lines always yield the same
 * colors whatever order they arrive in. A caller can label a row or a legend
 * entry with the color its line was drawn in by calling this with the same
 * lines.
 *
 * Up to 360 lines the colors are all distinct, beyond which
 * `generateRainbowPalette` repeats.
 */
export function assignLineColors<Datum>(
  lines: ThresholdLineSeries<Datum>[],
): Map<string, string> {
  const ranked = [...lines].sort(
    (a, b) =>
      (b.breachValue ?? 0) - (a.breachValue ?? 0) || a.key.localeCompare(b.key),
  );
  const palette = generateRainbowPalette(ranked.length);

  const colors = new Map<string, string>();
  ranked.forEach((line, rank) => colors.set(line.key, palette[rank]));
  return colors;
}

/**
 * Snap a raw value to the control's step and clamp it into range.
 *
 * Rounds to the step's decimal places as well as to its multiple: a bare
 * `Math.round(v / step) * step` reintroduces binary floating point error, and
 * the result of this is rendered as a label and compared against the data.
 */
export function quantizeThreshold(
  value: number,
  { step, min, max }: { step: number; min: number; max: number },
): number {
  const clamped = Math.min(Math.max(value, min), max);
  const snapped = Math.round(clamped / step) * step;
  const decimals = decimalPlaces(step);
  return Math.min(Math.max(parseFloat(snapped.toFixed(decimals)), min), max);
}

function decimalPlaces(step: number): number {
  // An exponent-form step such as 1e-7 has no "." to split on, and its
  // precision is the exponent.
  const exponential = step.toExponential().match(/e-(\d+)$/);
  if (exponential) {
    const significand = step.toExponential().split("e")[0];
    const significandDecimals = significand.split(".")[1]?.length ?? 0;
    return parseInt(exponential[1], 10) + significandDecimals;
  }
  return String(step).split(".")[1]?.length ?? 0;
}
