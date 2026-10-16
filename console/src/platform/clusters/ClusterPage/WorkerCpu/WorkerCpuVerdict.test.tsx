// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { DatabaseObject } from "~/api/materialize/objects";
import { renderComponent } from "~/test/utils";

import {
  CPU_DOCS,
  CpuVerdict,
  objectCpuRows,
  WorkerReplica,
} from "./workerCpuModel";
import {
  FirstLook,
  FirstLookProps,
  Verdict,
  VerdictProps,
} from "./WorkerCpuVerdict";

const SECOND_NS = 1e9;

const REPLICA: WorkerReplica = {
  id: "u2",
  name: "r1",
  size: "400cc",
  processes: 1,
  workersPerProcess: 4,
  cpuNanoCoresPerProcess: 4e9,
};

const ORDERS_INDEX: DatabaseObject = {
  id: "u5",
  name: "orders_idx",
  objectType: "index",
  schemaId: "u3",
  schemaName: "public",
  databaseId: "u1",
  databaseName: "materialize",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u1",
  clusterName: "quickstart",
};

const OBJECTS_BY_ID = new Map([[ORDERS_INDEX.id, ORDERS_INDEX]]);

const objectName = (objectId: string) =>
  OBJECTS_BY_ID.get(objectId)?.name ?? objectId;

// The index's dataflow took most of worker 0's time.
const INDEX_ON_WORKER_0 = objectCpuRows({
  latest: [50, 5, 5, 5].map((seconds, workerId) => ({
    dataflowId: "7",
    workerId,
    elapsedNs: seconds * SECOND_NS,
  })),
  exports: [{ objectId: ORDERS_INDEX.id, dataflowId: "7" }],
  workerCount: 4,
});

const saturated = (): CpuVerdict => ({
  kind: "saturated",
  busiest: { workerId: 0, busyFraction: 0.95 },
  averageBusyFraction: 0.31,
  othersBusyFraction: 0.1,
});

const renderVerdict = (
  props: Pick<VerdictProps, "verdict"> & Partial<VerdictProps>,
) =>
  renderComponent(
    <Verdict
      clusterName="quickstart"
      replica={REPLICA}
      windowMs={60_000}
      replicaCpuFraction={undefined}
      objects={undefined}
      objectsSinceMs={undefined}
      objectName={objectName}
      objectsById={OBJECTS_BY_ID}
      {...props}
    />,
  );

const renderFirstLook = (props: Partial<FirstLookProps>) =>
  renderComponent(
    <FirstLook
      replica={REPLICA}
      workerCount={4}
      measuredMs={10_000}
      latestCpu={undefined}
      lastReading={undefined}
      lifetimeObjects={undefined}
      objectName={objectName}
      {...props}
    />,
  );

describe("Verdict", () => {
  it("offers a copyable EXPLAIN ANALYZE, pinned to the replica, for the index on the saturated worker", async () => {
    await renderVerdict({
      verdict: saturated(),
      replicaCpuFraction: 0.9,
      objects: INDEX_ON_WORKER_0,
    });

    expect(
      screen.getByText(/EXPLAIN ANALYZE CPU WITH SKEW FOR INDEX/).textContent,
    ).toBe(
      [
        'SET cluster = "quickstart";',
        'SET cluster_replica = "r1";',
        'EXPLAIN ANALYZE CPU WITH SKEW FOR INDEX "materialize"."public"."orders_idx";',
      ].join("\n"),
    );
    expect(screen.getByRole("button", { name: "Copy text" })).toBeVisible();
  });

  it("attributes high CPU mostly outside running dataflows and links to ad-hoc query troubleshooting", async () => {
    await renderVerdict({
      verdict: {
        kind: "highCpu",
        replicaCpuFraction: 0.9,
        dataflowFraction: 0.2,
        outsideFraction: 0.7,
        busiest: { workerId: 1, busyFraction: 0.25 },
      },
      replicaCpuFraction: 0.9,
    });

    expect(
      screen.getByText(
        "r1 is using 90% of its CPU, mostly outside its running dataflows.",
      ),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: "Troubleshoot ad-hoc queries" }),
    ).toHaveAttribute("href", `${CPU_DOCS}#ad-hoc-query-load`);
  });

  it("does not suggest splitting clusters when dataflows use most of the high CPU", async () => {
    await renderVerdict({
      verdict: {
        kind: "highCpu",
        replicaCpuFraction: 0.9,
        dataflowFraction: 0.8,
        outsideFraction: 0.1,
        busiest: { workerId: 1, busyFraction: 0.85 },
      },
      replicaCpuFraction: 0.9,
    });

    expect(screen.getByText(/mostly running dataflows/)).toBeVisible();
    expect(screen.queryByText(/smaller clusters/)).not.toBeInTheDocument();
  });

  it("does not call the replica's CPU low for a saturated worker on a replica at 90%", async () => {
    await renderVerdict({ verdict: saturated(), replicaCpuFraction: 0.9 });

    expect(screen.getByText(/Work routed to it waits\./)).toBeVisible();
    expect(screen.queryByText(/only at/)).not.toBeInTheDocument();
  });

  it("points out a saturated worker behind a replica at 30% of its CPU", async () => {
    await renderVerdict({ verdict: saturated(), replicaCpuFraction: 0.3 });

    expect(
      screen.getByText(
        /Work routed to it waits, even though r1 is only at 30% of its CPU\./,
      ),
    ).toBeVisible();
  });
});

describe("FirstLook", () => {
  it("names the worker that has done more than twice the others' average over its dataflows' lifetimes", async () => {
    await renderFirstLook({
      lastReading: {
        atMs: Date.now(),
        elapsedNsByWorker: new Map(
          [300, 50, 50, 0].map((seconds, workerId) => [
            workerId,
            seconds * SECOND_NS,
          ]),
        ),
      },
    });

    expect(
      screen.getByText(
        /worker 0 has done 9\.0× as much as the other workers on average/,
      ),
    ).toBeVisible();
  });

  it("leaves out a replica CPU metric older than 3 minutes", async () => {
    await renderFirstLook({
      latestCpu: { atMs: Date.now() - 5 * millisecondsInMinute, percent: 80 },
    });

    expect(screen.queryByText(/of its CPU/)).not.toBeInTheDocument();
  });

  it("states a replica CPU metric from the last 3 minutes", async () => {
    await renderFirstLook({
      latestCpu: { atMs: Date.now() - millisecondsInMinute, percent: 80 },
    });

    expect(screen.getByText("r1 is at 80% of its CPU.")).toBeVisible();
  });
});
