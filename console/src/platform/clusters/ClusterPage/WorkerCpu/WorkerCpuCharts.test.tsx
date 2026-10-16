// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { renderComponent } from "~/test/utils";

import { CHART_MARGIN } from "../ResourceUsage/resourceUsageStyles";
import { WorkerCpuCharts, WorkerCpuChartsProps } from "./WorkerCpuCharts";
import { CpuPoint, WorkerBusyPoint, WorkerReplica } from "./workerCpuModel";

const WIDTH_PX = 800;
const PLOT_WIDTH_PX = WIDTH_PX - CHART_MARGIN.left - CHART_MARGIN.right;
const START_MS = Date.UTC(2026, 9, 16, 12);
const END_MS = START_MS + 15 * millisecondsInMinute;
const OPENED_AT_MS = START_MS + 10 * millisecondsInMinute;

const REPLICA: WorkerReplica = {
  id: "u2",
  name: "r1",
  size: "400cc",
  color: "#7c5cff",
  allocation: {
    processes: 1,
    workers: 4,
    cpuNanoCores: 4e9,
    memoryBytes: 0,
    diskBytes: 0,
  },
};

// Worker readings start 30s after opening. Replica CPU covers the whole
// window up to then.
const SERIES: WorkerBusyPoint[] = [0, 5, 10, 15].map((seconds) => ({
  atMs: OPENED_AT_MS + 30_000 + seconds * 1000,
  busiest: { workerId: 2, busyFraction: 0.8 },
  averageBusyFraction: 0.4,
}));
const REPLICA_CPU: CpuPoint[] = Array.from({ length: 11 }, (_, minute) => ({
  atMs: START_MS + minute * millisecondsInMinute,
  percent: 50,
}));

const renderCharts = (props: Partial<WorkerCpuChartsProps> = {}) =>
  renderComponent(
    <WorkerCpuCharts
      width={WIDTH_PX}
      startMs={START_MS}
      endMs={END_MS}
      openedAtMs={OPENED_AT_MS}
      replicaCpu={REPLICA_CPU}
      series={SERIES}
      replica={REPLICA}
      measured={undefined}
      {...props}
    />,
  );

const xOf = (timeMs: number) =>
  CHART_MARGIN.left +
  ((timeMs - START_MS) / (END_MS - START_MS)) * PLOT_WIDTH_PX;

const hoverAt = async (timeMs: number) => {
  const svg = screen.getByRole("img", { name: /CPU and its workers' time/ });
  const overlay = svg.querySelector('rect[fill="transparent"]');
  if (!overlay) throw new Error("hover overlay not rendered");
  fireEvent.pointerMove(overlay, { clientX: xOf(timeMs), clientY: 40 });
  return screen.findByTestId("chart-tooltip");
};

const numberAttribute = (element: Element, name: string) =>
  Number(element.getAttribute(name));

describe("WorkerCpuCharts", () => {
  beforeAll(() => {
    // jsdom has no PointerEvent, and the fallback Event drops clientX.
    if (!window.PointerEvent) {
      window.PointerEvent = class extends MouseEvent {} as typeof PointerEvent;
    }
  });

  it("names the replica's CPU, the workers' time, and the CPU between them in the legend", async () => {
    await renderCharts();

    expect(
      screen.getAllByRole("listitem").map((item) => item.textContent),
    ).toEqual(["Replica CPU", "Workers' time", "Other CPU"]);
  });

  it("shades the measured span", async () => {
    const measured = {
      startMs: OPENED_AT_MS + 30_000,
      endMs: OPENED_AT_MS + 90_000,
    };
    await renderCharts({ measured });

    const span = screen.getByTestId("measured-span");
    expect(numberAttribute(span, "x")).toBeCloseTo(xOf(measured.startMs));
    expect(numberAttribute(span, "width")).toBeCloseTo(
      xOf(measured.endMs) - xOf(measured.startMs),
    );
    expect(
      screen.getByText(
        "% of r1's allocation. The shaded span is the one measured above",
      ),
    ).toBeVisible();
  });

  it("clips a measured span that starts before the chart to the plot", async () => {
    await renderCharts({
      measured: {
        startMs: START_MS - millisecondsInMinute,
        endMs: START_MS + millisecondsInMinute,
      },
    });

    const span = screen.getByTestId("measured-span");
    expect(numberAttribute(span, "x")).toBe(CHART_MARGIN.left);
    expect(numberAttribute(span, "width")).toBeCloseTo(
      xOf(START_MS + millisecondsInMinute) - CHART_MARGIN.left,
    );
  });

  it("shades nothing before a measurement", async () => {
    await renderCharts();

    expect(screen.queryByTestId("measured-span")).not.toBeInTheDocument();
    expect(screen.getByText("% of r1's allocation")).toBeVisible();
  });

  it("shows the replica's CPU and the workers' time nearest the hovered time", async () => {
    await renderCharts();

    const tooltip = await hoverAt(SERIES[1].atMs + 1_000);

    expect(tooltip).toHaveTextContent(/Replica CPU\s*50% of allocation/);
    expect(tooltip).toHaveTextContent(/Workers' time\s*40% of allocation/);
  });

  it("counts the workers' time against the allocation, at two workers to a core", async () => {
    await renderCharts({
      replica: {
        ...REPLICA,
        allocation: { ...REPLICA.allocation, cpuNanoCores: 2e9 },
      },
    });

    const tooltip = await hoverAt(SERIES[1].atMs);

    expect(tooltip).toHaveTextContent(/Workers' time\s*80% of allocation/);
  });

  it("finds no worker reading minutes before the first one", async () => {
    await renderCharts();

    const tooltip = await hoverAt(SERIES[0].atMs - 9 * millisecondsInMinute);

    expect(tooltip).toHaveTextContent("No worker reading at this time.");
    expect(tooltip).toHaveTextContent(/Replica CPU\s*50% of allocation/);
    expect(tooltip).not.toHaveTextContent("Workers' time");
  });

  it("reads the replica's CPU for the minute that holds the hovered time, not the nearest minute's start", async () => {
    await renderCharts({
      replicaCpu: [
        { atMs: START_MS + 9 * millisecondsInMinute, percent: 20 },
        { atMs: START_MS + 10 * millisecondsInMinute, percent: 80 },
      ],
    });

    const tooltip = await hoverAt(START_MS + 9 * millisecondsInMinute + 40_000);

    expect(tooltip).toHaveTextContent(/Replica CPU\s*20% of allocation/);
  });

  it("leaves out replica CPU when no sample covers the hovered minute", async () => {
    await renderCharts({ replicaCpu: REPLICA_CPU.slice(0, 9) });

    const tooltip = await hoverAt(SERIES[1].atMs);

    expect(tooltip).not.toHaveTextContent("Replica CPU");
    expect(tooltip).toHaveTextContent(/Workers' time\s*40% of allocation/);
  });
});
