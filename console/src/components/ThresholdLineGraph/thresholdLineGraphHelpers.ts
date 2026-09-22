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

/** 1 - 1/phi. The stride that spreads a walk around a circle most evenly. */
const GOLDEN_FRACTION = 0.381966;

/**
 * Evenly spaced hues around the color wheel, at a fixed saturation and
 * lightness.
 *
 * Generated to the size of the set it has to color rather than drawn from a
 * fixed list. A threshold can put any number of lines in play at once, and a
 * fixed palette answers that badly in both directions: it runs out, or it
 * forces distinct lines to share a color. Sizing the wheel to the set keeps
 * every line distinguishable and spreads the few-line case as far apart as the
 * wheel allows.
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
      `hsl(${Math.round((index * 360) / numColors)}, ${saturation}%, ${lightness}%)`,
  );
}

function greatestCommonDivisor(a: number, b: number): number {
  return b === 0 ? a : greatestCommonDivisor(b, a % b);
}

/**
 * The indices `0..count-1`, reordered so that any prefix of the result is
 * spread across the whole range rather than bunched at its start.
 *
 * Colors are handed out by breach rank, and lines adjacent in rank are exactly
 * the ones a reader most needs to tell apart: they are the worst offenders,
 * sitting next to each other at the top of the list. Taking hues in order
 * gives those lines near-identical colors once the set is large, because the
 * step between neighbours shrinks as the set grows. Striding the wheel instead
 * puts rank neighbours on opposite sides of it, and costs the small-set case
 * nothing, since a short walk still visits well separated hues.
 *
 * The stride is the nearest integer to the golden-ratio fraction of `count`
 * that is coprime with it. Coprimality makes the walk a permutation, visiting
 * every index exactly once, and the golden ratio is the fraction whose
 * successive multiples fill a circle most evenly.
 */
export function spreadIndices(count: number): number[] {
  if (count <= 0) return [];
  let stride = Math.max(1, Math.round(count * GOLDEN_FRACTION));
  while (greatestCommonDivisor(stride, count) !== 1) {
    stride += 1;
  }
  return Array.from({ length: count }, (_unused, i) => (i * stride) % count);
}

/** Whether the line's judged value sits strictly above the threshold. */
export function isBreaching<Datum>(
  line: ThresholdLineSeries<Datum>,
  threshold: number,
): boolean {
  return line.breachValue !== null && line.breachValue > threshold;
}

/**
 * A color for every line the reader should be looking at, keyed by series key.
 * A line absent from the map is context, and the graph draws it de-emphasized.
 *
 * Every breaching line gets a color of its own; none are ever left looking
 * like context, and no two share one. Hues are handed out worst breach first,
 * in the strided order `spreadIndices` gives, so the lines furthest over are
 * also the furthest apart in hue.
 *
 * Deterministic: callers rely on that to label a row or a legend entry with
 * the color its line was drawn in, by calling this with the same arguments
 * rather than plumbing the result back out of the graph. Ties break on key.
 *
 * NOTE: the palette is sized to the highlighted set, so changing the threshold
 * recolors the lines. That is the cost of never running out and never
 * repeating; a fixed assignment would have to do one or the other.
 *
 * A selected key is a union with the breaching set, never a replacement.
 * Picking a line by hand adds it to what the threshold already found, which is
 * what lets the two controls coexist without a mode to switch between them.
 */
export function assignHighlightColors<Datum>({
  lines,
  threshold,
  selectedKeys,
  saturation,
  lightness,
}: {
  lines: ThresholdLineSeries<Datum>[];
  threshold: number;
  selectedKeys?: ReadonlySet<string>;
  saturation?: number;
  lightness?: number;
}): Map<string, string> {
  const breaching = lines
    .filter((line) => isBreaching(line, threshold))
    .sort(
      (a, b) =>
        (b.breachValue ?? 0) - (a.breachValue ?? 0) ||
        a.key.localeCompare(b.key),
    );

  const picked = selectedKeys
    ? lines.filter(
        (line) => selectedKeys.has(line.key) && !isBreaching(line, threshold),
      )
    : [];

  const highlighted = [...breaching, ...picked];
  const palette = generateRainbowPalette(
    highlighted.length,
    saturation,
    lightness,
  );
  const order = spreadIndices(highlighted.length);

  const colors = new Map<string, string>();
  highlighted.forEach((line, rank) =>
    colors.set(line.key, palette[order[rank]]),
  );
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
