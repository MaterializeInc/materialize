// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { Meta, StoryObj } from "@storybook/react-vite";
import React from "react";

import {
  ThresholdLineGraph,
  ThresholdLineGraphProps,
} from "./ThresholdLineGraph";
import { ThresholdLineSeries } from "./types";

/** One sampled instant: every object's freshness reading at that time. */
interface Reading {
  time: number;
  values: Record<string, number | null>;
}

interface ObjectSpec {
  key: string;
  label: string;
  /** Seconds the line hovers around. */
  baseline: number;
  /** How far it swings either side of the baseline. */
  swing: number;
  /** Half-open range of point indices where the object reported nothing. */
  gap?: [number, number];
}

const START_TIME = Date.UTC(2026, 0, 15, 6, 0, 0);
const STEP_MS = 5 * 60 * 1000;
const POINT_COUNT = 72;
const END_TIME = START_TIME + (POINT_COUNT - 1) * STEP_MS;

/**
 * Seeded PRNG. Stories are visual fixtures, so the same story must draw the
 * same picture on every render, in every browser, forever. `Math.random` would
 * make each screenshot differ from the last and turn visual review into noise.
 */
function mulberry32(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function buildReadings(specs: ObjectSpec[]): Reading[] {
  const random = mulberry32(0x5eed);
  // Each object gets its own phase so the lines don't rise and fall in unison.
  const phases = new Map(
    specs.map((spec) => [spec.key, random() * Math.PI * 2]),
  );

  return Array.from({ length: POINT_COUNT }, (_unused, index) => ({
    time: START_TIME + index * STEP_MS,
    values: Object.fromEntries(
      specs.map((spec) => {
        if (spec.gap && index >= spec.gap[0] && index < spec.gap[1]) {
          return [spec.key, null];
        }
        const phase = phases.get(spec.key) ?? 0;
        const wave = Math.sin(index / 6 + phase);
        const jitter = (random() - 0.5) * spec.swing * 0.4;
        const value = spec.baseline + wave * spec.swing + jitter;
        return [spec.key, Math.max(0, Number(value.toFixed(2)))];
      }),
    ),
  }));
}

/** The peak reading, which is what the threshold judges each line against. */
function peakOf(readings: Reading[], key: string): number | null {
  const values = readings
    .map((reading) => reading.values[key])
    .filter((value): value is number => value !== null && value !== undefined);
  return values.length === 0 ? null : Math.max(...values);
}

function buildLines(
  specs: ObjectSpec[],
  readings: Reading[],
): ThresholdLineSeries<Reading>[] {
  return specs.map((spec) => ({
    key: spec.key,
    label: spec.label,
    yAccessor: (reading: Reading) => reading.values[spec.key] ?? null,
    breachValue: peakOf(readings, spec.key),
  }));
}

const SPECS: ObjectSpec[] = [
  { key: "u1", label: "analytics.orders_enriched", baseline: 0.8, swing: 0.3 },
  { key: "u2", label: "analytics.daily_revenue", baseline: 1.2, swing: 0.4 },
  { key: "u3", label: "public.session_rollup", baseline: 3.4, swing: 1.1 },
  { key: "u4", label: "public.inventory_levels", baseline: 0.5, swing: 0.2 },
  { key: "u5", label: "billing.invoice_lines", baseline: 5.2, swing: 1.6 },
  { key: "u6", label: "billing.usage_by_account", baseline: 2.1, swing: 0.6 },
];

const READINGS = buildReadings(SPECS);
const LINES = buildLines(SPECS, READINGS);

const MANY_SPECS: ObjectSpec[] = Array.from(
  { length: 24 },
  (_unused, index) => ({
    key: `m${index}`,
    label: `schema_${index % 4}.materialized_view_${index}`,
    baseline: 0.4 + index * 0.35,
    swing: 0.3 + (index % 5) * 0.2,
  }),
);
const MANY_READINGS = buildReadings(MANY_SPECS);
const MANY_LINES = buildLines(MANY_SPECS, MANY_READINGS);

const GAPPY_SPECS: ObjectSpec[] = [
  { key: "g1", label: "public.steady_view", baseline: 1.5, swing: 0.4 },
  {
    key: "g2",
    label: "public.restarted_view",
    baseline: 4.0,
    swing: 0.9,
    gap: [20, 38],
  },
  {
    key: "g3",
    label: "public.late_arriving_view",
    baseline: 2.6,
    swing: 0.7,
    gap: [0, 30],
  },
];
const GAPPY_READINGS = buildReadings(GAPPY_SPECS);
const GAPPY_LINES = buildLines(GAPPY_SPECS, GAPPY_READINGS);

function formatSeconds(value: number) {
  return Number.isInteger(value) ? `${value}s` : `${value.toFixed(1)}s`;
}

/**
 * The graph is controlled, so a story that let the arg drive `threshold`
 * directly would render a handle that cannot be dragged. This holds the value
 * the way a real caller does, while still following the arg when the Controls
 * panel changes it.
 */
const ThresholdLineGraphHarness = (props: ThresholdLineGraphProps<Reading>) => {
  const [threshold, setThreshold] = React.useState(props.threshold);

  React.useEffect(() => setThreshold(props.threshold), [props.threshold]);

  return (
    <ThresholdLineGraph
      {...props}
      threshold={threshold}
      onThresholdChange={setThreshold}
    />
  );
};

const meta = {
  title: "Components/ThresholdLineGraph",
  component: ThresholdLineGraphHarness,
  args: {
    data: READINGS,
    lines: LINES,
    xAccessor: (reading: Reading) => reading.time,
    startTime: START_TIME,
    endTime: END_TIME,
    threshold: 2,
    onThresholdChange: () => undefined,
    formatValue: formatSeconds,
    thresholdLabel: "Freshness threshold",
    graphLabel: "Freshness over time for every object on the cluster",
    height: 320,
  },
  argTypes: {
    threshold: { control: { type: "range", min: 0, max: 12, step: 0.1 } },
    height: { control: { type: "range", min: 160, max: 600, step: 20 } },
    data: { control: false },
    lines: { control: false },
    xAccessor: { control: false },
    formatValue: { control: false },
    onThresholdChange: { control: false },
  },
} satisfies Meta<typeof ThresholdLineGraphHarness>;

export default meta;

type Story = StoryObj<typeof meta>;

/** Two of six objects sit above the threshold and get a color of their own. */
export const Default: Story = {};

/** No line breaches, so the whole set stays grey. */
export const NothingBreaching: Story = {
  args: { threshold: 11 },
};

/** Every line breaches, which is the widest the generated palette has to go. */
export const EverythingBreaching: Story = {
  args: { threshold: 0 },
};

/**
 * Twenty-four lines over the threshold at once. The palette is generated to
 * fit however many breach, so no line is dropped and no two share a color.
 */
export const ManyBreachingLines: Story = {
  args: {
    data: MANY_READINGS,
    lines: MANY_LINES,
    threshold: 1,
    height: 420,
  },
};

/**
 * A null reading breaks its line rather than bridging the gap, so an object
 * that was down reads as absent instead of flat.
 */
export const GapsInData: Story = {
  args: {
    data: GAPPY_READINGS,
    lines: GAPPY_LINES,
    threshold: 3,
  },
};

/**
 * `selectedKeys` unions with whatever breaches, so a hand-picked object keeps
 * its color as the threshold moves past it.
 */
export const HandPickedSelection: Story = {
  args: {
    threshold: 6,
    selectedKeys: new Set(["u1", "u4"]),
  },
};

/** No objects on the cluster yet: axes only, with the handle still draggable. */
export const NoData: Story = {
  args: {
    data: [],
    lines: [],
    threshold: 1,
  },
};
