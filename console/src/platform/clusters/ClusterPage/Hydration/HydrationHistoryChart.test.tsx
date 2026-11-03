// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { millisecondsInHour, millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { renderComponent } from "~/test/utils";

import { CHART_MARGIN } from "../ResourceUsage/resourceUsageStyles";
import { HydrationHistoryChart } from "./HydrationHistoryChart";
import { HydrationRow } from "./hydrationView";

const WIDTH_PX = 900;

vi.mock("@visx/responsive", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@visx/responsive")>()),
  useParentSize: () => ({
    parentRef: () => {},
    width: WIDTH_PX,
    height: 0,
    resize: () => {},
  }),
}));

const END_MS = Date.UTC(2026, 9, 27);
const START_MS = END_MS - 168 * millisecondsInHour;

const hydration = (replicaId: string, startedAtMs: number): HydrationRow => ({
  type: "hydration",
  id: `${replicaId}|${startedAtMs}`,
  replicaId,
  startedAt: new Date(startedAtMs),
  finishedAt: new Date(startedAtMs + millisecondsInMinute),
  durationMs: millisecondsInMinute,
  objectCount: 3,
  peakHeapBytes: null,
  peakMemoryBytes: null,
  peakDiskBytes: null,
  kind: "full",
});

const renderChart = (rows: HydrationRow[], nameById: Map<string, string>) =>
  renderComponent(
    <HydrationHistoryChart
      rows={rows}
      inProgress={[]}
      replicaIds={[...nameById.keys()]}
      colorById={new Map()}
      nameById={nameById}
      startMs={START_MS}
      endMs={END_MS}
      selectedId={undefined}
      onSelect={() => {}}
    />,
  );

const markXs = () =>
  screen
    .getAllByRole("button")
    .map((mark) =>
      parseFloat(mark.querySelector("circle")!.getAttribute("cx")!),
    );

describe("HydrationHistoryChart", () => {
  it("draws the hydrations of a replica replaced every hour in the plot, in time order", async () => {
    // Newest first, as the tab lists replicas.
    const rows = Array.from({ length: 168 }, (_, index) =>
      hydration(`u${168 - index}`, END_MS - (index + 0.5) * millisecondsInHour),
    );
    await renderChart(
      rows,
      new Map(rows.map(({ replicaId }) => [replicaId, "r1"])),
    );

    const xs = markXs();
    expect(xs).toHaveLength(168);
    expect(Math.min(...xs)).toBeGreaterThanOrEqual(CHART_MARGIN.left);
    expect(Math.max(...xs)).toBeLessThanOrEqual(WIDTH_PX - CHART_MARGIN.right);
    expect(xs).toEqual([...xs].sort((a, b) => b - a));
  });

  it("sets apart replicas of different names hydrating at once, within a bounded spread", async () => {
    const atMs = END_MS - 84 * millisecondsInHour;
    const names = (count: number) =>
      new Map(
        Array.from({ length: count }, (_, index) => [
          `u${index + 1}`,
          `r${index + 1}`,
        ]),
      );
    const rowsFor = (nameById: Map<string, string>) =>
      [...nameById.keys()].map((replicaId) => hydration(replicaId, atMs));

    const pair = names(2);
    const { unmount } = await renderChart(rowsFor(pair), pair);
    const [first, second] = markXs();
    expect(Math.abs(first - second)).toBe(16);
    unmount();

    const many = names(10);
    await renderChart(rowsFor(many), many);
    const xs = markXs();
    expect(new Set(xs).size).toBe(10);
    expect(Math.max(...xs) - Math.min(...xs)).toBeLessThanOrEqual(48);
  });
});
