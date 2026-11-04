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

import { renderComponent } from "~/test/utils";

import { WorkerBars, WorkerBarsProps } from "./WorkerBars";
import { Measurement, MEASUREMENT_MS, WorkerReplica } from "./workerCpuModel";

const SECOND_NS = 1e9;

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

// Two processes of two workers each.
const TWO_PROCESS_REPLICA: WorkerReplica = {
  ...REPLICA,
  allocation: { ...REPLICA.allocation, processes: 2, workers: 2 },
};

/** A minute in which each worker was `busy` for that share of its core. */
const buildMeasurement = (busy: number[]): Measurement => ({
  startMs: 0,
  endMs: MEASUREMENT_MS,
  workers: busy
    .map((busyFraction, workerId) => ({ workerId, busyFraction }))
    .sort((a, b) => b.busyFraction - a.busyFraction || a.workerId - b.workerId),
  rows: [],
  workerTotalsNs: busy.map((fraction) => fraction * MEASUREMENT_MS * 1e6),
});

const SKEWED = buildMeasurement([0.95, 0.1, 0.1, 0.1]);

const renderBars = (props: Partial<WorkerBarsProps> = {}) =>
  renderComponent(
    <WorkerBars
      width={800}
      measurement={SKEWED}
      picked={undefined}
      hotWorkers={undefined}
      selectedWorkerId={0}
      onSelectWorker={() => {}}
      replica={REPLICA}
      {...props}
    />,
  );

/** The rects of a worker's bar: its whole time, then the picked object's part. */
const barRects = (workerId: number) => [
  ...document.querySelectorAll(`[data-bar="${workerId}"] rect`),
];

/** The area that hovers and selects a worker. */
const column = (workerId: number) => {
  const element = document.querySelector(`[data-worker-id="${workerId}"]`);
  if (!element) throw new Error(`no column for w${workerId}`);
  return element;
};

const fills = (rects: Element[]) =>
  rects.map((rect) => rect.getAttribute("fill"));

const PICKED = {
  name: "orders_idx",
  perWorkerNs: [50, 5, 5, 5].map((seconds) => seconds * SECOND_NS),
};

describe("WorkerBars", () => {
  it("summarizes the busiest worker against the others' average for screen readers", async () => {
    await renderBars();

    expect(
      screen.getByRole("img", {
        name: "4 workers. The busiest, w0, was 95% busy, and the other workers averaged 10%.",
      }),
    ).toBeVisible();
  });

  it("draws one bar per worker, with the busiest worker's share above it", async () => {
    await renderBars();

    expect(document.querySelectorAll("[data-bar]")).toHaveLength(4);
    expect(document.querySelectorAll("[data-worker-id]")).toHaveLength(4);
    expect(screen.getAllByText(/^w\d$/)).toHaveLength(4);
    expect(screen.getByText("95%")).toBeInTheDocument();
  });

  it("draws the dashed average level with the tops of the other workers' bars and states it in the legend", async () => {
    await renderBars();

    const [w1] = barRects(1);
    const average = screen
      .getByRole("img", { name: /^4 workers/ })
      .querySelector('line[stroke-dasharray="4 3"]');
    expect(Number(average?.getAttribute("y1"))).toBeCloseTo(
      Number(w1.getAttribute("y")),
    );
    expect(
      screen.getByText("The other workers' average (10%)"),
    ).toBeInTheDocument();
  });

  it("shows the picked object's part of each worker's time, in the bar and on hover", async () => {
    await renderBars({ picked: PICKED });

    const [whole, part] = barRects(0);
    expect(Number(part.getAttribute("y"))).toBeGreaterThan(
      Number(whole.getAttribute("y")),
    );
    expect(screen.getByText("orders_idx")).toBeInTheDocument();
    expect(
      screen.getByText("The rest of each worker's time"),
    ).toBeInTheDocument();

    fireEvent.pointerEnter(column(0));
    const tooltip = screen.getByTestId("chart-tooltip");
    expect(tooltip).toHaveTextContent("w0");
    expect(tooltip).toHaveTextContent("Busy95% of a core");
    expect(tooltip).toHaveTextContent("orders_idx83%");
  });

  it("never draws the picked object's part past its worker's bar", async () => {
    await renderBars({
      measurement: buildMeasurement([1, 0.1, 0.1, 0.1]),
      picked: {
        name: "orders_idx",
        perWorkerNs: [62, 5, 5, 5].map((seconds) => seconds * SECOND_NS),
      },
    });

    const [whole, part] = barRects(0);
    expect(Number(part.getAttribute("y"))).toBeCloseTo(
      Number(whole.getAttribute("y")),
    );
  });

  it("labels each worker's whole time when nothing is picked", async () => {
    await renderBars();

    expect(screen.getByText("Each worker's time")).toBeInTheDocument();
    expect(barRects(0)).toHaveLength(1);
  });

  it("colors the called-out worker apart from the others, under the verdict's name for it", async () => {
    await renderBars({
      hotWorkers: { workerIds: [0], label: "Worker at its core's limit" },
    });

    expect(fills(barRects(0))).not.toEqual(fills(barRects(1)));
    expect(screen.getByText("Worker at its core's limit")).toBeInTheDocument();
    expect(screen.queryByText("Overloaded worker")).not.toBeInTheDocument();
  });

  it("colors every called-out worker alike, apart from the rest", async () => {
    await renderBars({
      hotWorkers: { workerIds: [0, 2], label: "Workers at their core's limit" },
    });

    expect(fills(barRects(2))).toEqual(fills(barRects(0)));
    expect(fills(barRects(2))).not.toEqual(fills(barRects(1)));
    expect(
      screen.getByText("Workers at their core's limit"),
    ).toBeInTheDocument();
  });

  it("colors every worker alike without an overloaded one", async () => {
    await renderBars();

    expect(fills(barRects(0))).toEqual(fills(barRects(1)));
    expect(screen.queryByText("Overloaded worker")).not.toBeInTheDocument();
  });

  it("names a hovered worker's process and says clicking ranks the objects by its time", async () => {
    await renderBars({ replica: TWO_PROCESS_REPLICA });

    fireEvent.pointerEnter(column(3));
    const tooltip = screen.getByTestId("chart-tooltip");
    expect(tooltip).toHaveTextContent("w3 (process 1)");
    expect(tooltip).toHaveTextContent(
      "Click to rank the objects below by its time.",
    );

    fireEvent.pointerLeave(column(3));
    expect(screen.queryByTestId("chart-tooltip")).not.toBeInTheDocument();
  });

  it("selects the worker whose column is clicked", async () => {
    const onSelectWorker = vi.fn();
    await renderBars({ onSelectWorker });

    fireEvent.click(column(2));

    expect(onSelectWorker).toHaveBeenCalledWith(2);
  });

  it("moves the selection with the arrow keys while the chart has focus, wrapping around", async () => {
    const onSelectWorker = vi.fn();
    await renderBars({ selectedWorkerId: 3, onSelectWorker });

    const chart = screen.getByRole("group", { name: /w3 is selected/ });
    fireEvent.keyDown(chart, { key: "ArrowRight" });
    expect(onSelectWorker).toHaveBeenLastCalledWith(0);
    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    expect(onSelectWorker).toHaveBeenLastCalledWith(2);
  });

  it("groups the bars by process on a replica with more than one", async () => {
    await renderBars({ replica: TWO_PROCESS_REPLICA });

    expect(screen.getByText("Process 0")).toBeInTheDocument();
    expect(screen.getByText("Process 1")).toBeInTheDocument();
  });

  it("leaves out process labels on a single-process replica", async () => {
    await renderBars();

    expect(screen.queryByText(/^Process/)).not.toBeInTheDocument();
  });

  it("draws one worker without an average to compare it with", async () => {
    await renderBars({
      measurement: buildMeasurement([0.4]),
      replica: {
        ...REPLICA,
        allocation: { ...REPLICA.allocation, workers: 1 },
      },
    });

    expect(
      screen.getByRole("img", { name: "1 worker. w0 was 40% busy." }),
    ).toBeVisible();
    expect(
      screen.queryByText(/The other workers' average/),
    ).not.toBeInTheDocument();
    expect(
      screen
        .getByRole("img", { name: /^1 worker/ })
        .querySelector('line[stroke-dasharray="4 3"]'),
    ).not.toBeInTheDocument();
  });

  it("labels the busiest and selected workers even when narrow bars label only some, and keeps their neighbors' labels off them", async () => {
    const busy = Array.from({ length: 32 }, (_, workerId) =>
      workerId === 7 ? 0.9 : 0.2,
    );
    await renderBars({
      width: 400,
      measurement: buildMeasurement(busy),
      selectedWorkerId: 19,
    });

    expect(screen.getByText("w7")).toBeInTheDocument();
    expect(screen.getByText("w19")).toBeInTheDocument();
    expect(screen.queryByText("w18")).not.toBeInTheDocument();
    expect(screen.getAllByText(/^w\d+$/).length).toBeLessThan(32);
  });
});
