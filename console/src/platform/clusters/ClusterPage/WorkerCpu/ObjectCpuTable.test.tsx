// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";

import { renderComponent } from "~/test/utils";

import { ObjectCpuTable, ObjectCpuTableProps } from "./ObjectCpuTable";
import { objectCpuRows, WorkerReplica } from "./workerCpuModel";

const SECOND_NS = 1e9;

const REPLICA: WorkerReplica = {
  id: "u2",
  name: "r1",
  size: "400cc",
  processes: 1,
  workersPerProcess: 4,
  cpuNanoCoresPerProcess: 4e9,
};

const perWorker = (dataflowId: string, seconds: number[]) =>
  seconds.map((s, workerId) => ({
    dataflowId,
    workerId,
    elapsedNs: s * SECOND_NS,
  }));

const renderTable = (
  latest: ReturnType<typeof perWorker>,
  props: Partial<ObjectCpuTableProps> = {},
) => {
  const { rows, workerTotalsNs } = objectCpuRows({
    latest,
    exports: [
      { objectId: "u5", dataflowId: "1" },
      { objectId: "u6", dataflowId: "2" },
    ],
    workerCount: 4,
  });
  return renderComponent(
    <ObjectCpuTable
      rows={rows}
      sortBy="total"
      mode="window"
      canMeasureWindow
      sinceMs={0}
      onModeChange={() => {}}
      dataflowTotalNs={workerTotalsNs.reduce((sum, ns) => sum + ns, 0)}
      allocation={undefined}
      replica={REPLICA}
      objectName={(objectId) => objectId}
      objectsById={new Map()}
      {...props}
    />,
  );
};

describe("ObjectCpuTable", () => {
  it("measures a window against the replica's allocation and says what ran outside dataflows", async () => {
    // 60s on 4 cores is 240s of allocation, of which dataflows used 48s.
    await renderTable(perWorker("1", [12, 12, 12, 12]), {
      allocation: { ns: 240 * SECOND_NS, replicaCpuFraction: 0.5 },
    });

    expect(screen.getByText("% of replica CPU")).toBeVisible();
    expect(screen.queryByText("Share of dataflow CPU")).toBeNull();
    expect(screen.getByText("20%")).toBeVisible();
    expect(
      screen.getByText(/Work outside running dataflows took 30%/),
    ).toBeVisible();
  });

  it("measures a lifetime against all dataflow time", async () => {
    await renderTable(perWorker("1", [12, 12, 12, 12]), { mode: "lifetime" });

    expect(screen.getByText("Share of dataflow CPU")).toBeVisible();
    expect(screen.queryByText("% of replica CPU")).toBeNull();
  });

  it("doesn't flag skew in an object too small to matter", async () => {
    // u6 sits on one worker but has well under 1% of the dataflow time.
    await renderTable([
      ...perWorker("1", [1000, 1000, 1000, 1000]),
      ...perWorker("2", [1, 0, 0, 0]),
    ]);

    expect(screen.queryByText("Extra time")).toBeNull();
  });

  it("flags skew in an object with a real share of the time", async () => {
    await renderTable([
      ...perWorker("1", [1000, 1000, 1000, 1000]),
      ...perWorker("2", [400, 0, 0, 0]),
    ]);

    expect(screen.getByText("Extra time")).toBeVisible();
  });
});
