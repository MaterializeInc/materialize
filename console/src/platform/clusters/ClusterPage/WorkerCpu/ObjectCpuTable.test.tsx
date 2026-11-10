// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import parsePostgresInterval from "postgres-interval";
import React from "react";

import { LagMap } from "~/api/materialize/cluster/materializationLag";
import { DatabaseObject } from "~/api/materialize/objects";
import {
  buildDatabaseObject,
  renderComponent,
  RenderWithPathname,
} from "~/test/utils";
import { lightTheme } from "~/theme";

import { ObjectCpuTable, ObjectCpuTableProps } from "./ObjectCpuTable";
import {
  ADHOC_ROW_ID,
  DataflowExport,
  Measurement,
  MEASUREMENT_MS,
  objectCpuRows,
  SYSTEM_ROW_ID,
} from "./workerCpuModel";

const SECOND_NS = 1e9;
// A minute on 4 cores.
const ALLOCATION_NS = 4 * MEASUREMENT_MS * 1e6;

const OBJECTS: DatabaseObject[] = [
  buildDatabaseObject({ id: "u5", name: "orders_idx", objectType: "index" }),
  buildDatabaseObject({
    id: "u6",
    name: "payments_mv",
    objectType: "materialized-view",
  }),
  buildDatabaseObject({ id: "u7", name: "shipments_idx", objectType: "index" }),
];
const OBJECTS_BY_ID = new Map(OBJECTS.map((object) => [object.id, object]));

const objectName = (objectId: string) =>
  objectId === ADHOC_ROW_ID
    ? "Queries and subscribes"
    : objectId === SYSTEM_ROW_ID
      ? "Materialize's own dataflows"
      : (OBJECTS_BY_ID.get(objectId)?.name ?? objectId);

interface DataflowSeconds {
  export: DataflowExport;
  /** By worker id. */
  seconds: number[];
}

const exported = (objectId: string, seconds: number[]): DataflowSeconds => ({
  export: { objectId, dataflowId: objectId },
  seconds,
});

const adhoc = (dataflowId: string, seconds: number[]): DataflowSeconds => ({
  export: {
    objectId: `t${dataflowId}`,
    dataflowId,
    dataflowName: `Dataflow: subscribe-${dataflowId}`,
  },
  seconds,
});

/** A minute in which each worker was `busy` for that share of its core. */
const buildMeasurement = (
  busy: number[],
  dataflows: DataflowSeconds[],
): Measurement => {
  const { rows } = objectCpuRows({
    latest: dataflows.flatMap(({ export: { dataflowId }, seconds }) =>
      seconds.map((s, workerId) => ({
        dataflowId,
        workerId,
        elapsedNs: s * SECOND_NS,
      })),
    ),
    exports: dataflows.map((dataflow) => dataflow.export),
    workerCount: busy.length,
  });
  return {
    startMs: 0,
    endMs: MEASUREMENT_MS,
    workers: busy
      .map((busyFraction, workerId) => ({ workerId, busyFraction }))
      .sort((a, b) => b.busyFraction - a.busyFraction),
    rows,
    workerTotalsNs: busy.map((fraction) => fraction * MEASUREMENT_MS * 1e6),
  };
};

// w0 spent all 60s of the minute running work, the others 18s each.
// orders_idx has the most of w0's time, all of its own work on w0, and
// payments_mv the most time overall, spread evenly.
const MEASUREMENT = buildMeasurement(
  [1, 0.3, 0.3, 0.3],
  [
    exported("u5", [33, 0, 0, 0]),
    exported("u6", [12, 12, 12, 12]),
    exported("u7", [10, 3, 3, 3]),
    adhoc("100", [1, 1, 0, 0]),
    adhoc("101", [0, 0, 1, 1]),
  ],
);

const QUERY_HISTORY = {
  startedPath: "/regions/aws-us-east-1/query-history?sort_field=duration",
  runningPath:
    "/regions/aws-us-east-1/query-history?finished_statuses%5B%5D=running",
};

const renderTable = (props: Partial<ObjectCpuTableProps> = {}) =>
  renderComponent(
    <RenderWithPathname>
      <ObjectCpuTable
        measurement={MEASUREMENT}
        workerId={0}
        pickedId={undefined}
        onPick={() => {}}
        allocationNs={ALLOCATION_NS}
        lagMap={undefined}
        objectName={objectName}
        objectsById={OBJECTS_BY_ID}
        isSkewed
        queryHistory={QUERY_HISTORY}
        {...props}
      />
    </RenderWithPathname>,
  );

/** The object rows, in table order, without the header and footer. */
const objectRows = () => {
  const [, body] = screen.getAllByRole("rowgroup");
  return within(body).getAllByRole("row");
};

/** The name in a row's first cell, above its type or count. */
const nameOf = (row: HTMLElement) =>
  within(within(row).getAllByRole("cell")[0]).getAllByText(/./)[0].textContent;

const rowOf = (name: string) => {
  const row = objectRows().find((candidate) => nameOf(candidate) === name);
  if (!row) throw new Error(`no row for ${name}`);
  return row;
};

/** The text of each of a row's cells, in column order. */
const cellsOf = (row: HTMLElement) =>
  within(row)
    .getAllByRole("cell")
    .map((cell) => cell.textContent);

const objectOrder = () => objectRows().map(nameOf);

describe("ObjectCpuTable", () => {
  it("heads the time column with the selected worker", async () => {
    await renderTable({ workerId: 2 });

    expect(
      screen.getAllByRole("columnheader").map((header) => header.textContent),
    ).toEqual([
      "Object",
      "Time on w2",
      "Share of replica CPU",
      "Spread across workers",
      "Behind its inputs",
    ]);
  });

  it("leaves out the spread column for a measurement of one worker", async () => {
    await renderTable({
      measurement: buildMeasurement(
        [0.6],
        [exported("u5", [20]), exported("u6", [10])],
      ),
    });

    expect(
      screen.getAllByRole("columnheader").map((header) => header.textContent),
    ).toEqual([
      "Object",
      "Time on w0",
      "Share of replica CPU",
      "Behind its inputs",
    ]);
    expect(cellsOf(rowOf("orders_idx"))).toEqual([
      "orders_idxIndex",
      "56% of w0",
      "8%",
      "-",
    ]);
  });

  it("ranks objects by their time on the given worker, not their total time", async () => {
    await renderTable();

    expect(objectOrder()).toEqual([
      "orders_idx",
      "payments_mv",
      "shipments_idx",
      "Queries and subscribes",
    ]);
  });

  it("re-ranks by share of replica CPU when its header is clicked", async () => {
    await renderTable();

    await userEvent.click(
      screen.getByRole("columnheader", { name: /Share of replica CPU/ }),
    );

    expect(objectOrder()).toEqual([
      "payments_mv",
      "orders_idx",
      "shipments_idx",
      "Queries and subscribes",
    ]);
  });

  it("ranks objects by another worker's time when that worker is given", async () => {
    await renderTable({ workerId: 1 });

    expect(objectOrder()).toEqual([
      "payments_mv",
      "shipments_idx",
      "Queries and subscribes",
      "orders_idx",
    ]);
    expect(cellsOf(rowOf("payments_mv"))[1]).toBe("67% of w1");
  });

  it("shows each object's time on the given worker, share of the allocation, and spread", async () => {
    await renderTable();

    // orders_idx: 33s of w0's 60s, 33s of the 240s allocation.
    expect(cellsOf(rowOf("orders_idx")).slice(0, 4)).toEqual([
      "orders_idxIndex",
      "55% of w0",
      "14%",
      "All on w0",
    ]);
    expect(cellsOf(rowOf("payments_mv")).slice(0, 4)).toEqual([
      "payments_mvMaterialized View",
      "20% of w0",
      "20%",
      "Even",
    ]);
    expect(cellsOf(rowOf("shipments_idx")).slice(0, 4)).toEqual([
      "shipments_idxIndex",
      "17% of w0",
      "8%",
      "Mostly w0 · 3.3× the others",
    ]);
  });

  it("pools queries and subscribes in one row that counts them", async () => {
    await renderTable();

    expect(cellsOf(rowOf("Queries and subscribes"))).toEqual([
      "Queries and subscribes2 dataflows for sessions' statementsQuery History: started in these minutes · still running",
      "2% of w0",
      "2%",
      "Even",
      "-",
    ]);
  });

  it("puts the time of dataflows that export no object in an Other dataflows footer", async () => {
    await renderTable();

    const footer = screen.getByText("Other dataflows").closest("tr");
    if (!footer) throw new Error("no footer row");
    // w0 spent 56s of its 60s on the rows above, and the workers 114s in all,
    // of which the rows took 104s.
    expect(cellsOf(footer).slice(0, 3)).toEqual([
      "Other dataflowsDataflows that export no object",
      "7% of w0",
      "4%",
    ]);
  });

  it("pools the coordinator's own dataflows in a row of their own, without a link or freshness", async () => {
    await renderTable({
      measurement: buildMeasurement(
        [1, 0.3, 0.3, 0.3],
        [
          exported("u5", [33, 0, 0, 0]),
          {
            export: {
              objectId: "t30",
              dataflowId: "30",
              dataflowName: "Dataflow: introspection-subscribe-t30",
            },
            seconds: [3, 3, 3, 3],
          },
        ],
      ),
    });

    const row = rowOf("Materialize's own dataflows");
    expect(cellsOf(row)).toEqual([
      "Materialize's own dataflowsIntrospection and metrics, on every replica",
      "5% of w0",
      "5%",
      "Even",
      "-",
    ]);
    expect(within(row).queryByRole("link")).not.toBeInTheDocument();
  });

  it("shows how far each object is behind its inputs", async () => {
    const lagMap: LagMap = new Map([
      [
        "u5",
        {
          lag: parsePostgresInterval("00:01:33"),
          hydrated: true,
          isOutdated: true,
        },
      ],
      ["u6", { lag: null, hydrated: false, isOutdated: false }],
    ]);
    await renderTable({ lagMap });

    expect(within(rowOf("orders_idx")).getByText("1m 33s")).toBeVisible();
    expect(
      within(rowOf("payments_mv")).getByText("Not hydrated"),
    ).toBeVisible();
    expect(within(rowOf("shipments_idx")).getByText("-")).toBeVisible();
  });

  it("colors skew in an object with a real share of the time, and not in one too small to matter", async () => {
    // u8 sits on one worker but has well under 1% of the workers' time.
    await renderTable({
      measurement: buildMeasurement(
        [1, 0.3, 0.3, 0.3],
        [exported("u5", [33, 0, 0, 0]), exported("u8", [0.5, 0, 0, 0])],
      ),
    });

    expect(within(rowOf("orders_idx")).getByText("All on w0")).toHaveStyle({
      color: lightTheme.colors.accent.red,
    });
    expect(within(rowOf("u8")).getByText("All on w0")).toHaveStyle({
      color: lightTheme.colors.foreground.secondary,
    });
  });

  it("doesn't color a leaning object when the verdict found no skew", async () => {
    await renderTable({ isSkewed: false });

    expect(within(rowOf("orders_idx")).getByText("All on w0")).toHaveStyle({
      color: lightTheme.colors.foreground.secondary,
    });
  });

  it("picks an object from a click anywhere in its row", async () => {
    const onPick = vi.fn();
    await renderTable({ onPick });

    await userEvent.click(within(rowOf("payments_mv")).getByText("Even"));
    expect(onPick).toHaveBeenLastCalledWith("u6");

    await userEvent.click(within(rowOf("orders_idx")).getByText("Index"));
    expect(onPick).toHaveBeenLastCalledWith("u5");

    await userEvent.click(
      screen.getByText("2 dataflows for sessions' statements"),
    );
    expect(onPick).toHaveBeenLastCalledWith(ADHOC_ROW_ID);
    expect(onPick).toHaveBeenCalledTimes(3);
  });

  it("picks an object from the keyboard through its time on the worker, which says whether it's picked", async () => {
    const onPick = vi.fn();
    await renderTable({ onPick, pickedId: "u6" });

    const toggle = screen.getByRole("button", {
      name: "55% of w0. Show orders_idx on the bars.",
    });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    expect(
      screen.getByRole("button", {
        name: "20% of w0. Show payments_mv on the bars.",
      }),
    ).toHaveAttribute("aria-pressed", "true");
    toggle.focus();
    await userEvent.keyboard("{Enter}");

    expect(onPick).toHaveBeenCalledTimes(1);
    expect(onPick).toHaveBeenLastCalledWith("u5");
  });

  it("marks only the picked object's row as picked", async () => {
    await renderTable({ pickedId: "u6" });

    expect(
      screen
        .getAllByRole("button", { pressed: true })
        .map((button) => button.getAttribute("aria-label")),
    ).toEqual(["20% of w0. Show payments_mv on the bars."]);
  });

  it("links each catalog object's name to its page, which opens without picking it", async () => {
    const onPick = vi.fn();
    await renderTable({ onPick });

    const link = within(rowOf("orders_idx")).getByRole("link", {
      name: "orders_idx",
    });
    const href = link.getAttribute("href");
    expect(href).toContain("u5");
    await userEvent.click(link);

    expect(screen.getByTestId("pathname")).toHaveTextContent(href ?? "");
    expect(onPick).not.toHaveBeenCalled();
  });

  it("links the queries row to its statements in Query History, those that started in the span and those still running, without picking it", async () => {
    const onPick = vi.fn();
    await renderTable({ onPick });

    const row = within(rowOf("Queries and subscribes"));
    expect(
      row.queryByRole("link", { name: "Queries and subscribes" }),
    ).not.toBeInTheDocument();
    expect(row.getByRole("link", { name: "still running" })).toHaveAttribute(
      "href",
      QUERY_HISTORY.runningPath,
    );
    await userEvent.click(
      row.getByRole("link", { name: "started in these minutes" }),
    );

    expect(screen.getByTestId("pathname")).toHaveTextContent(
      "/regions/aws-us-east-1/query-history",
    );
    expect(onPick).not.toHaveBeenCalled();
  });

  it("offers no link for an object missing from the catalog", async () => {
    await renderTable({
      measurement: buildMeasurement(
        [1, 0.3, 0.3, 0.3],
        [exported("u9", [10, 1, 1, 1])],
      ),
    });

    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("pages objects past the first ten", async () => {
    await renderTable({
      measurement: buildMeasurement(
        [1, 1],
        Array.from({ length: 11 }, (_, index) =>
          exported(`u${100 + index}`, [1 + index / 10, 1]),
        ),
      ),
    });

    expect(objectOrder()).toHaveLength(10);
    expect(screen.getByText("Showing 1-10 of 11 objects")).toBeVisible();

    await userEvent.click(screen.getByRole("button", { name: "Next page" }));

    expect(objectOrder()).toEqual(["u100"]);
  });

  it("says when no object did measurable work", async () => {
    await renderTable({ measurement: buildMeasurement([0.1, 0.1], []) });

    expect(
      screen.getByText("No object did measurable work in this span."),
    ).toBeVisible();
  });
});
