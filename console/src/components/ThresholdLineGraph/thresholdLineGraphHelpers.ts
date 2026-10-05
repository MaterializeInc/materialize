// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { hcl } from "d3";

import { clamp } from "~/util";

import { ThresholdLineSeries } from "./types";

const DEFAULT_CHROMA = 50;
const DEFAULT_LIGHTNESS = 65;

/**
 * Bit-reversal (van der Corput) sequence over 360 degrees, which halves its
 * spacing as it grows and does not depend on the count.
 *
 *   index   0     1     2     3     4     5     6     7
 *   hue     0   180    90   270    45   225   135   315
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
 * Colors furthest-apart first, sized on demand, since a fixed palette either
 * runs out or repeats.
 *
 * HCL so one lightness is one perceived lightness, at a chroma low enough to
 * stay inside sRGB: clamping a vivid hue drags it back toward its neighbours.
 */
export function generateRainbowPalette(
  numColors: number,
  chroma: number = DEFAULT_CHROMA,
  lightness: number = DEFAULT_LIGHTNESS,
): string[] {
  return Array.from({ length: numColors }, (_unused, index) =>
    hcl(hueAtIndex(index), chroma, lightness).formatHex(),
  );
}

/**
 * Whether the line's judged value reaches the threshold.
 *
 * Inclusive, so a value exactly at the threshold breaches. The shaded band
 * stops at the threshold line, so a point sitting on it is drawn touching the
 * band, and anything else would have it look breaching while counting as
 * healthy. Monitoring's Objects page reads its own threshold the same way.
 */
export function isBreaching<Datum>(
  line: ThresholdLineSeries<Datum>,
  threshold: number,
): boolean {
  return line.breachValue !== null && line.breachValue >= threshold;
}

/**
 * A color per line, ranked worst `breachValue` first so whatever a threshold
 * highlights is furthest apart in hue. Deterministic, ties breaking on key.
 *
 * NOTE: color is rank, not identity, so it survives the threshold moving but
 * not the data changing.
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
 * Snap a value to the control's step and clamp it into range. Rounds to the
 * step's decimal places too, since `Math.round(v / step) * step` alone leaves
 * floating point error in a value that gets rendered as a label.
 */
export function quantizeThreshold(
  value: number,
  { step, min, max }: { step: number; min: number; max: number },
): number {
  const snapped = Math.round(clamp(value, min, max) / step) * step;
  return clamp(parseFloat(snapped.toFixed(decimalPlaces(step))), min, max);
}

function decimalPlaces(step: number): number {
  // An exponent-form step such as 1e-7 has no "." to split on.
  const exponential = step.toExponential().match(/e-(\d+)$/);
  if (exponential) {
    const significand = step.toExponential().split("e")[0];
    return (
      parseInt(exponential[1], 10) + (significand.split(".")[1]?.length ?? 0)
    );
  }
  return String(step).split(".")[1]?.length ?? 0;
}
