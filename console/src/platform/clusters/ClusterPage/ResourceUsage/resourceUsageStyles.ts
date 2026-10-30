// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import { scaleLinear } from "@visx/scale";

import { MaterializeTheme } from "~/theme";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import { ReplicaState } from "./resourceUsageModel";

/**
 * Shared by every chart so their plot areas line up on one time axis. The
 * right margin holds the percent labels.
 */
export const CHART_MARGIN = { top: 0, right: 40, bottom: 0, left: 0 };

export const PERCENT_TICKS = [0, 25, 50, 75, 100];

const PERCENT_TICK_STEP = 25;
// Clear space above the highest reading, which a chart's header floats over.
const HEADROOM_PERCENT = 25;
const MIN_AXIS_TOP_PERCENT = 125;
const MAX_PERCENT_TICK_STEPS = 7;

/** The first 25% line at least 25 points above `peak`, and never under 125%. */
export const percentAxisTop = (peak: number) =>
  Math.max(
    MIN_AXIS_TOP_PERCENT,
    Math.ceil((peak + HEADROOM_PERCENT) / PERCENT_TICK_STEP) *
      PERCENT_TICK_STEP,
  );

/**
 * The labeled lines strictly between 0 and `top`: every 25%, or every 50%,
 * 75% and so on once an axis spans more than seven 25% steps.
 */
export const percentAxisTicks = (top: number) => {
  const step =
    Math.ceil(top / PERCENT_TICK_STEP / MAX_PERCENT_TICK_STEPS) *
    PERCENT_TICK_STEP;
  const ticks: number[] = [];
  for (let tick = step; tick < top; tick += step) ticks.push(tick);
  return ticks;
};

/** A scale from 0 to `percentAxisTop(peak)`. */
export const percentScale = (
  peak: number,
  layout: ReturnType<typeof buildXYGraphLayoutProps>,
) => {
  const top = percentAxisTop(peak);
  return {
    yScale: scaleLinear<number>({
      domain: [0, top],
      range: layout.yScaleRange,
    }),
    ticks: percentAxisTicks(top),
  };
};

/**
 * Wide windows merge buckets until a full bucket spans at least this many
 * pixels per bar charted at one time.
 */
export const MIN_BAR_SLOT_PX = 10;

export const useStatusStyles = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return {
    running: { label: "running", color: colors.accent.green, opacity: 0.45 },
    hydrating: {
      label: "hydrating",
      color: colors.accent.darkYellow,
      opacity: 0.7,
    },
    offline: {
      label: "offline",
      color: colors.foreground.tertiary,
      opacity: 0.45,
    },
  } satisfies Record<
    ReplicaState,
    { label: string; color: string; opacity: number }
  >;
};
