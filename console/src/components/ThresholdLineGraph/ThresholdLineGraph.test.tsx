// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { renderComponent } from "~/test/utils";

import { ThresholdLineGraph } from "./ThresholdLineGraph";

// ParentSize measures a DOM box, and jsdom lays nothing out, so without this
// the graph renders at zero width and every geometric assertion is vacuous.
vi.mock("@visx/responsive/lib/components/ParentSize", () => ({
  default: ({
    children,
  }: {
    children: (size: { width: number; height: number }) => React.ReactNode;
  }) => <>{children({ width: 800, height: 240 })}</>,
}));

type Point = { time: number; over: number; under: number };

const START = 1_700_000_000_000;
const DATA: Point[] = [0, 1, 2].map((i) => ({
  time: START + i * 60_000,
  over: 5,
  under: 1,
}));

const LINES = [
  {
    key: "over",
    label: "over",
    yAccessor: (d: Point) => d.over,
    breachValue: 5,
  },
  {
    key: "under",
    label: "under",
    yAccessor: (d: Point) => d.under,
    breachValue: 1,
  },
];

function renderGraph(overrides: { threshold?: number; onChange?: () => void }) {
  return renderComponent(
    <ThresholdLineGraph
      data={DATA}
      lines={LINES}
      xAccessor={(d) => d.time}
      startTime={START}
      endTime={START + 120_000}
      threshold={overrides.threshold ?? 2}
      onThresholdChange={overrides.onChange ?? (() => {})}
      formatValue={(v) => `${v}s`}
      thresholdLabel="Freshness threshold"
      graphLabel="Freshness over time"
    />,
  );
}

/**
 * The line paths, which are the only direct `path` children of the svg. The
 * axes and grid render theirs inside `g` wrappers.
 */
function linePaths(container: HTMLElement) {
  return [...container.querySelectorAll("svg > path")];
}

describe("ThresholdLineGraph", () => {
  it("draws a line per series, emphasizing only the breaching ones", async () => {
    const { container } = await renderGraph({ threshold: 2 });

    const paths = linePaths(container);
    expect(paths).toHaveLength(2);

    const strokeWidths = paths.map((p) => p.getAttribute("stroke-width"));
    expect(strokeWidths.sort()).toEqual(["1", "2"]);
  });

  it("re-sorts which lines are emphasized when the threshold moves", async () => {
    const { container } = await renderGraph({ threshold: 10 });

    expect(
      linePaths(container).map((p) => p.getAttribute("stroke-width")),
    ).toEqual(["1", "1"]);
  });

  it("gives every breaching line its own color", async () => {
    const { container } = await renderGraph({ threshold: 0 });

    const paths = linePaths(container);
    expect(paths.map((p) => p.getAttribute("stroke-width"))).toEqual([
      "2",
      "2",
    ]);
    const strokes = paths.map((p) => p.getAttribute("stroke"));
    expect(new Set(strokes).size).toBe(2);
  });

  it("exposes the threshold as a slider carrying its formatted value", async () => {
    await renderGraph({ threshold: 2 });

    const slider = screen.getByRole("slider", { name: "Freshness threshold" });
    expect(slider).toHaveAttribute("aria-valuenow", "2");
    expect(slider).toHaveAttribute("aria-valuetext", "2s");
    expect(slider).toHaveAttribute("aria-valuemin", "0");
  });

  it("tops the y axis at a round value covering the tallest plotted value", async () => {
    await renderGraph({ threshold: 2 });

    // The tallest series is a flat 5, and the slider's ceiling is the axis top.
    expect(screen.getByRole("slider")).toHaveAttribute("aria-valuemax", "5");
  });

  it("does not move the y axis when the threshold changes", async () => {
    const low = await renderGraph({ threshold: 1 });
    const lowMax = low.getByRole("slider").getAttribute("aria-valuemax");
    low.unmount();

    const high = await renderGraph({ threshold: 4 });
    expect(high.getByRole("slider")).toHaveAttribute("aria-valuemax", lowMax);
  });

  it("nudges the threshold by one step on an arrow key", async () => {
    const onChange = vi.fn();
    await renderGraph({ threshold: 2, onChange });

    fireEvent.keyDown(screen.getByRole("slider"), { key: "ArrowUp" });
    expect(onChange).toHaveBeenCalledWith(2.1);

    fireEvent.keyDown(screen.getByRole("slider"), { key: "ArrowDown" });
    expect(onChange).toHaveBeenCalledWith(1.9);
  });

  it("jumps to each end of the range on Home and End", async () => {
    const onChange = vi.fn();
    await renderGraph({ threshold: 2, onChange });

    fireEvent.keyDown(screen.getByRole("slider"), { key: "Home" });
    expect(onChange).toHaveBeenCalledWith(0);

    onChange.mockClear();
    fireEvent.keyDown(screen.getByRole("slider"), { key: "End" });
    // The axis top, which the graph derives rather than the caller supplying.
    const axisTop = screen.getByRole("slider").getAttribute("aria-valuemax");
    expect(onChange.mock.calls[0][0]).toBe(parseFloat(axisTop ?? ""));
  });

  it("does not report a change for a key it does not handle", async () => {
    const onChange = vi.fn();
    await renderGraph({ threshold: 2, onChange });

    fireEvent.keyDown(screen.getByRole("slider"), { key: "a" });
    expect(onChange).not.toHaveBeenCalled();
  });
});
