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
import parsePostgresInterval from "postgres-interval";
import React from "react";

import { LagMap } from "~/api/materialize/cluster/materializationLag";
import { DatabaseObject } from "~/api/materialize/objects";
import { buildDatabaseObject, renderComponent } from "~/test/utils";

import { processesTabLink } from "../tabLinks";
import {
  ADHOC_ROW_ID,
  CPU_DOCS,
  cpuVerdict,
  Measurement,
  MEASUREMENT_MS,
  objectCpuRows,
  ONE_WORKER_RATIO,
  WorkerReplica,
  workersPerCore,
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
  color: "#7c5cff",
  allocation: {
    processes: 1,
    workers: 4,
    cpuNanoCores: 4e9,
    memoryBytes: 0,
    diskBytes: 0,
  },
};

// Two processes of two workers each, so w0 and w1 run on process 0.
const TWO_PROCESS_REPLICA: WorkerReplica = {
  ...REPLICA,
  allocation: { ...REPLICA.allocation, processes: 2, workers: 2 },
};

const ONE_WORKER_REPLICA: WorkerReplica = {
  ...REPLICA,
  size: "100cc",
  allocation: { ...REPLICA.allocation, workers: 1, cpuNanoCores: 1e9 },
};

const ORDERS_INDEX = buildDatabaseObject({
  id: "u5",
  name: "orders_idx",
  objectType: "index",
});
const PAYMENTS_VIEW = buildDatabaseObject({
  id: "u6",
  name: "payments_mv",
  objectType: "materialized-view",
});
const EVENTS_SOURCE = buildDatabaseObject({
  id: "u7",
  name: "events",
  objectType: "source",
  sourceType: "kafka",
});

const OBJECTS_BY_ID = new Map<string, DatabaseObject>(
  [ORDERS_INDEX, PAYMENTS_VIEW, EVENTS_SOURCE].map((object) => [
    object.id,
    object,
  ]),
);

const objectName = (objectId: string) =>
  OBJECTS_BY_ID.get(objectId)?.name ?? objectId;

/**
 * A minute in which each worker was `busy` for that share of its core and
 * spent `objects`' seconds on each object, by worker id.
 */
const buildMeasurement = (
  busy: number[],
  objects: Record<string, number[]> = {},
): Measurement => {
  const entries = Object.entries(objects);
  const { rows } = objectCpuRows({
    latest: entries.flatMap(([, seconds], dataflow) =>
      seconds.map((s, workerId) => ({
        dataflowId: String(dataflow),
        workerId,
        elapsedNs: s * SECOND_NS,
      })),
    ),
    exports: entries.map(([objectId], dataflow) => ({
      objectId,
      dataflowId: String(dataflow),
    })),
    workerCount: busy.length,
  });
  return {
    startMs: 0,
    endMs: MEASUREMENT_MS,
    workers: busy
      .map((busyFraction, workerId) => ({ workerId, busyFraction }))
      .sort(
        (a, b) => b.busyFraction - a.busyFraction || a.workerId - b.workerId,
      ),
    rows,
    workerTotalsNs: busy.map((fraction) => fraction * MEASUREMENT_MS * 1e6),
  };
};

const outdatedLag = (objectId: string, interval: string): LagMap =>
  new Map([
    [
      objectId,
      {
        lag: parsePostgresInterval(interval),
        hydrated: true,
        isOutdated: true,
      },
    ],
  ]);

// w0 is at 95% against 10% for the others, so the verdict is saturated.
const SATURATED_BUSY = [0.95, 0.1, 0.1, 0.1];

const QUERIES_PATH = "/regions/aws-us-east-1/query-history?cluster_id=u1";

/** Renders the verdict the Workers tab would reach for `measurement`. */
const renderVerdict = ({
  measurement,
  ...props
}: Pick<VerdictProps, "measurement"> &
  Partial<Omit<VerdictProps, "verdict">>) => {
  const replica = props.replica ?? REPLICA;
  const verdict = cpuVerdict(
    measurement.workers,
    props.replicaCpuFraction,
    workersPerCore(replica.allocation),
  );
  return renderComponent(
    <Verdict
      clusterName="quickstart"
      replica={replica}
      verdict={verdict}
      measurement={measurement}
      replicaCpuFraction={undefined}
      lagMap={undefined}
      objectName={objectName}
      objectsById={OBJECTS_BY_ID}
      hasSourcesOrSinks={false}
      queriesPath={QUERIES_PATH}
      {...props}
    />,
  );
};

const heading = (name: string | RegExp) =>
  screen.getByRole("heading", { name });

describe("Verdict", () => {
  describe("on skewed work", () => {
    it("names the overloaded worker and the object most of its time goes to", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
      });

      expect(
        heading("w0 is overloaded, and most of its time goes to orders_idx"),
      ).toBeVisible();
      expect(
        screen.getByText(
          "w0 is 95% busy. The other 3 workers average 10%, so w0 does 9.5× their work. 88% of w0's time goes to orders_idx, which runs 10.0× as much work on w0 as on the other workers.",
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "How to fix worker skew" }),
      ).toHaveAttribute("href", `${CPU_DOCS}#worker-skew`);
    });

    it("suspects a hot key when the object still gives the other workers some of its work", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
      });

      expect(screen.getByText(/usually means a hot key/)).toBeVisible();
      expect(screen.queryByText(/cross join/)).not.toBeInTheDocument();
    });

    it("suspects a cross join when the object runs all of its work on the overloaded worker", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 0, 0, 0] }),
      });

      expect(
        screen.getByText(/which runs all of its work on w0\./),
      ).toBeVisible();
      expect(screen.getByText(/what a cross join does/)).toBeVisible();
      expect(screen.queryByText(/hot key/)).not.toBeInTheDocument();
    });

    it("suspects a cross join once the object's ratio reaches ONE_WORKER_RATIO", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, {
          u5: [ONE_WORKER_RATIO, 1, 1, 1],
        }),
      });

      expect(screen.getByText(/what a cross join does/)).toBeVisible();
    });

    it("offers a copyable EXPLAIN ANALYZE, pinned to the replica, for the index on the overloaded worker", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
      });

      expect(
        screen.getByText(
          "To find the step in orders_idx's plan, and the key it splits on, run:",
        ),
      ).toBeVisible();
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

    it("explains a materialized view by its type", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u6: [50, 5, 5, 5] }),
      });

      expect(
        screen.getByText(
          /EXPLAIN ANALYZE CPU WITH SKEW FOR MATERIALIZED VIEW "materialize"\."public"\."payments_mv";/,
        ),
      ).toBeVisible();
    });

    it("offers no EXPLAIN ANALYZE for an object it can't explain", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u7: [50, 5, 5, 5] }),
      });

      expect(
        heading("w0 is overloaded, and most of its time goes to events"),
      ).toBeVisible();
      expect(screen.queryByText(/EXPLAIN ANALYZE/)).not.toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: "Copy text" }),
      ).not.toBeInTheDocument();
    });

    it("blames no single object when the overloaded worker's time is split across several", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, {
          u5: [20, 2, 2, 2],
          u6: [15, 2, 2, 2],
        }),
      });

      expect(
        heading("w0 is overloaded, with no single object behind it"),
      ).toBeVisible();
      expect(
        screen.getByText(
          /Its time is split across several objects\. The largest, orders_idx, takes 35% of it\./,
        ),
      ).toBeVisible();
      expect(screen.queryByText(/hot key|cross join/)).not.toBeInTheDocument();
      expect(screen.queryByText(/EXPLAIN ANALYZE/)).not.toBeInTheDocument();
    });

    it("says the worker does nearly all of the work when the other workers are idle", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.95, 0, 0, 0], { u5: [50, 0, 0, 0] }),
      });

      expect(
        screen.getByText(/so w0 does nearly all of the work\./),
      ).toBeVisible();
    });

    it("compares the busiest worker with the one other worker on a two-worker replica", async () => {
      await renderVerdict({
        replica: {
          ...REPLICA,
          allocation: { ...REPLICA.allocation, workers: 2 },
        },
        measurement: buildMeasurement([0.95, 0.1], { u5: [50, 5] }),
      });

      expect(
        screen.getByText(
          /w0 is 95% busy\. The other worker is at 10%, so w0 does 9\.5× its work\./,
        ),
      ).toBeVisible();
    });

    it("measures the object's lean on the worker it names, not on the object's own hottest worker", async () => {
      // u5's hottest worker is w1, 2.7× the others, but the finding names w0.
      await renderVerdict({
        measurement: buildMeasurement([1, 0.75, 0.1, 0.1], {
          u5: [40, 45, 5, 5],
        }),
      });

      expect(
        screen.getByText(
          /67% of w0's time goes to orders_idx, which runs 2\.2× as much work on w0 as on the other workers\./,
        ),
      ).toBeVisible();
    });

    it("says results aren't waiting yet when work leans on a worker short of its limit", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.6, 0.2, 0.2, 0.2], {
          u5: [30, 3, 3, 3],
        }),
      });

      expect(heading("Work leans on w0, mostly from orders_idx")).toBeVisible();
      expect(
        screen.getByText(
          /No worker is at its limit yet, so results aren't waiting on it\./,
        ),
      ).toBeVisible();
    });

    it("names only the worker when uneven work has no single object behind it", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.6, 0.2, 0.2, 0.2], {
          u5: [10, 3, 3, 3],
          u6: [10, 3, 3, 3],
        }),
      });

      expect(heading("Work leans on w0")).toBeVisible();
    });

    it("says how far the object falls behind and that replica CPU hides the worker below 50%", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
        lagMap: outdatedLag("u5", "00:01:33"),
        replicaCpuFraction: 0.3,
      });

      expect(
        screen.getByText(
          "orders_idx is 1m 33s behind its inputs. Replica CPU reads only 30%, because the average across workers hides w0.",
        ),
      ).toBeVisible();
    });

    it("doesn't call replica CPU low at 50% or above", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
        replicaCpuFraction: 0.5,
      });

      expect(
        screen.queryByText(/Replica CPU reads only/),
      ).not.toBeInTheDocument();
    });

    it("leaves out freshness for an object that keeps up with its inputs", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
        lagMap: new Map([
          [
            "u5",
            {
              lag: parsePostgresInterval("00:00:02"),
              hydrated: true,
              isOutdated: false,
            },
          ],
        ]),
      });

      expect(screen.queryByText(/behind its inputs/)).not.toBeInTheDocument();
    });

    it("quotes neither replica nor process CPU when the replica reports less than its workers ran", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.1, 0.1, 0.95, 0.1], {
          u5: [5, 5, 50, 5],
        }),
        replica: TWO_PROCESS_REPLICA,
        replicaCpuFraction: 0,
        busiestProcess: {
          processId: "1",
          value: 0,
          othersMean: 0,
          otherCount: 1,
        },
      });

      expect(heading(/^w2 is overloaded/)).toBeVisible();
      expect(
        screen.queryByText(/Replica CPU reads only/),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByText(/used CPU|used the most CPU/),
      ).not.toBeInTheDocument();
    });

    it("places the overloaded worker in its process and links to the replica's processes", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.1, 0.1, 0.95, 0.1], {
          u5: [5, 5, 50, 5],
        }),
        replica: TWO_PROCESS_REPLICA,
        // Agrees with the workers' time, so it reads as reported.
        replicaCpuFraction: 0.6,
        busiestProcess: {
          processId: "1",
          value: 61,
          othersMean: 34,
          otherCount: 1,
        },
        processesLink: processesTabLink("r1"),
      });

      expect(
        screen.getByText(/^w2, in process 1, is 95% busy\./),
      ).toBeVisible();
      expect(
        screen.getByText(
          "Its process, process 1, used the most CPU: 61% against 34% for the other process.",
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "CPU and memory by process" }),
      ).toHaveAttribute("href", "/?replica=r1");
    });

    it("doesn't credit a process with the most CPU when the processes are even", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
        replica: TWO_PROCESS_REPLICA,
        // One worker of 62 at 100% lifts its process about a point.
        // Agrees with the workers' time, so it reads as reported.
        replicaCpuFraction: 0.6,
        busiestProcess: {
          processId: "1",
          value: 21,
          othersMean: 20,
          otherCount: 1,
        },
      });

      expect(
        screen.getByText(
          "The processes used CPU about evenly, 21% at most against 20% for the others, so the skew is between workers, not processes.",
        ),
      ).toBeVisible();
      expect(screen.queryByText(/used the most CPU/)).not.toBeInTheDocument();
    });

    it("names a different busiest process when the overloaded worker's process isn't it", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
        replica: TWO_PROCESS_REPLICA,
        // Agrees with the workers' time, so it reads as reported.
        replicaCpuFraction: 0.6,
        busiestProcess: {
          processId: "1",
          value: 70,
          othersMean: 40,
          otherCount: 1,
        },
      });

      expect(
        screen.getByText(
          "Process 1 used the most CPU, 70% against 40% for the other process.",
        ),
      ).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "CPU and memory by process" }),
      ).not.toBeInTheDocument();
    });

    it("leaves processes out on a single-process replica", async () => {
      await renderVerdict({
        measurement: buildMeasurement(SATURATED_BUSY, { u5: [50, 5, 5, 5] }),
      });

      expect(screen.queryByText(/process/i)).not.toBeInTheDocument();
    });
  });

  describe("on high replica CPU", () => {
    it("attributes high CPU mostly to work outside dataflows and links to ad-hoc query troubleshooting", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.25, 0.2, 0.15, 0.2], {
          u5: [5, 5, 5, 5],
        }),
        replicaCpuFraction: 0.9,
      });

      expect(
        heading("Not skew: r1 is at 90% of its CPU, mostly outside dataflows"),
      ).toBeVisible();
      expect(
        screen.getByText(
          /^Its workers spent 20% of the allocation running work, and the busiest, w0, 25% of its core, so this isn't skew\. The other 70% went to work outside the workers' dataflows: answering SELECTs straight from indexes, queries that started and ended between readings,/,
        ),
      ).toBeVisible();
      expect(
        screen.getByText(
          /orders_idx took the most, 42% of the workers' time, so start there\./,
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Troubleshoot ad-hoc queries" }),
      ).toHaveAttribute("href", `${CPU_DOCS}#ad-hoc-query-load`);
      expect(
        screen.getByRole("link", {
          name: "See quickstart's queries in Query History",
        }),
      ).toHaveAttribute("href", QUERIES_PATH);
      expect(
        screen.getByRole("link", {
          name: "Troubleshoot an undersized cluster",
        }),
      ).toHaveAttribute("href", `${CPU_DOCS}#an-undersized-cluster`);
      expect(screen.queryByText(/sources/)).not.toBeInTheDocument();
    });

    it("names source and sink work outside dataflows on a cluster that runs them", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.25, 0.2, 0.15, 0.2]),
        replicaCpuFraction: 0.9,
        hasSourcesOrSinks: true,
      });

      expect(
        screen.getByText(
          /outside the workers' dataflows: reading from sources and writing to sinks, answering SELECTs/,
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Troubleshoot upstream data volume" }),
      ).toHaveAttribute("href", `${CPU_DOCS}#upstream-data-volume`);
    });

    it("calls high CPU from evenly spread work not skew", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.65, 0.6, 0.6, 0.6]),
        replicaCpuFraction: 0.8,
      });

      expect(
        heading(
          "Not skew: r1 is at 80% of its CPU, spread evenly across its workers",
        ),
      ).toBeVisible();
      expect(
        screen.getByText(
          /Another 19% went to work outside the workers' dataflows\./,
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Troubleshoot an expensive object" }),
      ).toHaveAttribute("href", `${CPU_DOCS}#an-expensive-object`);
      expect(screen.queryByText(/smaller clusters/)).not.toBeInTheDocument();
    });

    it("doesn't call high CPU spread evenly when work leans on one worker", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.85, 0.4, 0.4, 0.4]),
        replicaCpuFraction: 0.8,
      });

      expect(heading("Not skew: r1 is at 80% of its CPU")).toBeVisible();
      expect(
        screen.getByText(
          /Work leans on w0, but no worker is at its limit, so skew isn't what uses the CPU\./,
        ),
      ).toBeVisible();
      expect(screen.queryByText(/spread evenly/)).not.toBeInTheDocument();
    });

    it("says every worker is busy once the workers' share reaches ALL_BUSY_FRACTION", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.95, 0.92, 0.93, 0.9]),
        replicaCpuFraction: 0.95,
      });

      expect(heading("Not skew: every worker is busy")).toBeVisible();
      expect(screen.queryByText(/Another/)).not.toBeInTheDocument();
    });

    it("says how far an outdated object falls behind", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.65, 0.6, 0.6, 0.6], {
          u5: [20, 20, 20, 20],
          u6: [5, 5, 5, 5],
        }),
        replicaCpuFraction: 0.8,
        lagMap: outdatedLag("u6", "00:00:45"),
      });

      expect(
        screen.getByText("payments_mv is 45s behind its inputs."),
      ).toBeVisible();
    });
  });

  describe("on a replica with one worker", () => {
    it("says its work can't be uneven, with how busy the worker was and the object that took the most of its time", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.5], { u5: [24], u6: [3] }),
        replica: ONE_WORKER_REPLICA,
      });

      expect(
        heading("r1 runs one worker, so its work can't be uneven"),
      ).toBeVisible();
      expect(
        screen.getByText(
          "w0 was 50% busy, and orders_idx took the most of its time, 80%.",
        ),
      ).toBeVisible();
      expect(screen.queryByRole("link")).not.toBeInTheDocument();
    });

    it("calls a busy lone worker not skew, and links to troubleshooting an expensive object or an undersized cluster", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.95], { u5: [45] }),
        replica: ONE_WORKER_REPLICA,
        lagMap: outdatedLag("u5", "00:01:00"),
      });

      expect(heading("Not skew: r1's only worker is busy")).toBeVisible();
      expect(
        screen.getByText(
          "w0 was 95% busy, and orders_idx took the most of its time, 79%. Less than 10% of its core is left idle.",
        ),
      ).toBeVisible();
      expect(
        screen.getByText("orders_idx is 1m behind its inputs."),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Troubleshoot an expensive object" }),
      ).toHaveAttribute("href", `${CPU_DOCS}#an-expensive-object`);
      expect(
        screen.getByRole("link", {
          name: "Troubleshoot an undersized cluster",
        }),
      ).toHaveAttribute("href", `${CPU_DOCS}#an-undersized-cluster`);
    });

    it("leads with high replica CPU short of a busy worker, with the share spent outside its dataflows", async () => {
      await renderVerdict({
        measurement: buildMeasurement([0.6], { u5: [24] }),
        replica: ONE_WORKER_REPLICA,
        replicaCpuFraction: 0.9,
      });

      expect(heading("Not skew: r1 is at 90% of its CPU")).toBeVisible();
      expect(
        screen.getByText(
          "w0 was 60% busy, and orders_idx took the most of its time, 67%. Another 30% of the replica's CPU went to work outside its worker's dataflows.",
        ),
      ).toBeVisible();
      expect(
        screen.getByRole("link", {
          name: "Troubleshoot an undersized cluster",
        }),
      ).toHaveAttribute("href", `${CPU_DOCS}#an-undersized-cluster`);
    });
  });

  it("says every worker is busy and starts with the object that takes the most", async () => {
    await renderVerdict({
      measurement: buildMeasurement([0.95, 0.92, 0.93, 0.9], {
        u5: [30, 30, 30, 30],
      }),
      lagMap: outdatedLag("u5", "00:02:00"),
    });

    expect(heading("Not skew: every worker is busy")).toBeVisible();
    expect(
      screen.getByText(
        "All 4 workers are 90% to 95% busy, so the work is already spread evenly and r1 has less than 10% headroom. orders_idx takes the most, 54% of the workers' time, so start there.",
      ),
    ).toBeVisible();
    expect(
      screen.getByText("orders_idx is 2m behind its inputs."),
    ).toBeVisible();
  });

  it("finds nothing to act on in lightly loaded work, and says to measure again if a leaning object grows", async () => {
    await renderVerdict({
      measurement: buildMeasurement([0.3, 0.1, 0.1, 0.1], {
        u5: [15, 1.5, 1.5, 1.5],
      }),
    });

    expect(heading("Nothing to act on")).toBeVisible();
    expect(
      screen.getByText(
        "Workers are lightly loaded: the busiest, w0, is 30% busy. orders_idx runs 10.0× as much work on w0 as on the other workers, but no worker is near its limit, so it isn't slowing anything down.",
      ),
    ).toBeVisible();
    expect(
      screen.getByText(
        "If orders_idx grows or starts falling behind, measure again.",
      ),
    ).toBeVisible();
  });

  it("doesn't speak of falling behind for the queries row, which has no freshness", async () => {
    const measurement = buildMeasurement([0.3, 0.1, 0.1, 0.1], {
      u5: [15, 1.5, 1.5, 1.5],
    });
    await renderVerdict({
      measurement: {
        ...measurement,
        rows: measurement.rows.map((row) => ({
          ...row,
          objectId: ADHOC_ROW_ID,
        })),
      },
      objectName: (objectId) =>
        objectId === ADHOC_ROW_ID ? "Queries and subscribes" : objectId,
    });

    expect(
      screen.getByText("If Queries and subscribes grows, measure again."),
    ).toBeVisible();
  });

  it("names no object in lightly loaded work when the top object is spread evenly", async () => {
    await renderVerdict({
      measurement: buildMeasurement([0.3, 0.1, 0.1, 0.1], {
        u5: [5, 4, 4, 4],
      }),
    });

    expect(
      screen.getByText(
        "Workers are lightly loaded: the busiest, w0, is 30% busy.",
      ),
    ).toBeVisible();
    expect(screen.queryByText(/measure again/)).not.toBeInTheDocument();
  });

  it("says work is spread evenly, with any object that falls behind", async () => {
    await renderVerdict({
      measurement: buildMeasurement([0.4, 0.35, 0.35, 0.35], {
        u5: [10, 10, 10, 10],
      }),
      lagMap: outdatedLag("u5", "00:00:30"),
    });

    expect(heading("Work is spread evenly")).toBeVisible();
    expect(
      screen.getByText(
        "The busiest worker, w0, is 40% busy, and the other workers average 35%. Skew isn't slowing r1 down.",
      ),
    ).toBeVisible();
    expect(
      screen.getByText("orders_idx is 30s behind its inputs."),
    ).toBeVisible();
  });

  it("says the workers are idle when they did no measurable work", async () => {
    await renderVerdict({ measurement: buildMeasurement([0, 0, 0, 0]) });

    expect(heading("Workers are idle")).toBeVisible();
    expect(
      screen.getByText(
        "No object did measurable work on r1 in this span. Skew only shows while workers are busy.",
      ),
    ).toBeVisible();
  });
});

const renderFirstLook = (props: Partial<FirstLookProps>) =>
  renderComponent(
    <FirstLook
      replica={REPLICA}
      workerCount={4}
      measuredMs={10_000}
      status="reading"
      latestCpu={undefined}
      lastReading={undefined}
      lifetimeObjects={undefined}
      objectName={objectName}
      {...props}
    />,
  );

const lifetimeReading = (seconds: number[]) => ({
  atMs: Date.now(),
  elapsedNsByWorker: new Map(
    seconds.map((s, workerId) => [workerId, s * SECOND_NS]),
  ),
});

describe("FirstLook", () => {
  it("names the worker that has done more than twice the others' average since each object started", async () => {
    await renderFirstLook({ lastReading: lifetimeReading([300, 50, 50, 0]) });

    expect(
      screen.getByText(
        "Since each object started, w0 has done 9.0× as much as the other workers on average.",
      ),
    ).toBeVisible();
  });

  it("says work has been spread evenly, with the object that took the most", async () => {
    // Dataflow 8 exports nothing, as a system dataflow.
    await renderFirstLook({
      lastReading: lifetimeReading([100, 100, 100, 100]),
      lifetimeObjects: objectCpuRows({
        latest: [
          { dataflowId: "7", seconds: 80 },
          { dataflowId: "8", seconds: 20 },
        ].flatMap(({ dataflowId, seconds }) =>
          [0, 1, 2, 3].map((workerId) => ({
            dataflowId,
            workerId,
            elapsedNs: seconds * SECOND_NS,
          })),
        ),
        exports: [{ objectId: ORDERS_INDEX.id, dataflowId: "7" }],
        workerCount: 4,
      }),
    });

    expect(
      screen.getByText(
        "Since each object started, the work is spread evenly across workers, and orders_idx accounts for 80% of the workers' time.",
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

  it("shows a spinner while measuring the first minute", async () => {
    await renderFirstLook({});

    expect(
      screen.getByText("Measuring r1's 4 workers for a minute (10s of 60s)"),
    ).toBeVisible();
    expect(screen.getByText("Loading...")).toBeInTheDocument();
  });

  it("counts a lone worker in the singular", async () => {
    await renderFirstLook({ replica: ONE_WORKER_REPLICA, workerCount: 1 });

    expect(
      screen.getByText("Measuring r1's 1 worker for a minute (10s of 60s)"),
    ).toBeVisible();
  });

  it("gives a lone worker's top object as a share of that worker's time", async () => {
    await renderFirstLook({
      replica: ONE_WORKER_REPLICA,
      workerCount: 1,
      lastReading: lifetimeReading([100]),
      lifetimeObjects: objectCpuRows({
        latest: [
          { dataflowId: "7", workerId: 0, elapsedNs: 75 * SECOND_NS },
          { dataflowId: "8", workerId: 0, elapsedNs: 25 * SECOND_NS },
        ],
        exports: [{ objectId: ORDERS_INDEX.id, dataflowId: "7" }],
        workerCount: 1,
      }),
    });

    expect(
      screen.getByText(
        "Since each object started, orders_idx accounts for 75% of its worker's time.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByText(/as much as the other workers/),
    ).not.toBeInTheDocument();
  });

  it("doesn't count progress past the measured minute", async () => {
    await renderFirstLook({ measuredMs: 90_000 });

    expect(
      screen.getByText(/^Measuring r1's 4 workers for a minute/),
    ).toBeVisible();
    expect(screen.queryByText(/1m 30s/)).not.toBeInTheDocument();
  });

  it("says measuring is paused, without a spinner, while paused", async () => {
    await renderFirstLook({ status: "paused" });

    expect(
      screen.getByText("Paused measuring r1's 4 workers (10s of 60s)"),
    ).toBeVisible();
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });

  it("says measuring failed, without a spinner, after a read failed", async () => {
    await renderFirstLook({ status: "error" });

    expect(
      screen.getByText("Couldn't finish measuring r1's 4 workers (10s of 60s)"),
    ).toBeVisible();
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });
});
