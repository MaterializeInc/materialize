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
import React from "react";
import { describe, expect, it } from "vitest";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import { renderComponent } from "~/test/utils";

import { FreshnessRow, UNREADABLE } from "./freshnessRows";
import { FreshnessTable } from "./FreshnessTable";

const buildRow = (overrides: Partial<FreshnessRow>): FreshnessRow => ({
  key: "u1",
  objectName: "orders_mv",
  namespace: "materialize.public",
  objectType: "materialized-view",
  hydratedReplicas: 2,
  totalReplicas: 2,
  current: 420,
  peak: 9_000,
  p90: 9_000,
  breachValue: 9_000,
  breaching: true,
  color: "#ff0000",
  ...overrides,
});

const rows: FreshnessRow[] = [
  buildRow({}),
  buildRow({
    key: "u2",
    objectName: "users_idx",
    objectType: "index",
    current: 300,
    peak: 380,
    p90: 380,
    breachValue: 380,
    breaching: false,
    color: undefined,
  }),
  buildRow({
    key: "u3",
    objectName: "legacy_sink",
    objectType: "sink",
    hydratedReplicas: 0,
    totalReplicas: 2,
    current: null,
    peak: null,
    p90: null,
    breachValue: null,
    breaching: false,
    color: undefined,
  }),
];

describe("FreshnessTable", () => {
  it("lists every object, over threshold or not", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    expect(await screen.findByText("orders_mv")).toBeVisible();
    expect(screen.getByText("users_idx")).toBeVisible();
    expect(screen.getByText("legacy_sink")).toBeVisible();
  });

  it("marks only the rows drawn on the graph", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    // The swatch is the only link between a row and its line, so an extra or a
    // missing one misattributes.
    const swatches = await screen.findAllByLabelText("Shown on the graph");
    expect(swatches).toHaveLength(1);

    const overRow = screen.getByText("orders_mv").closest("tr");
    expect(within(overRow!).getByLabelText("Shown on the graph")).toBeVisible();
  });

  it("renders a missing reading as a dash rather than a zero", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    const nullRow = (await screen.findByText("legacy_sink")).closest("tr");
    // An object with no reading has not been measured at zero lag.
    expect(within(nullRow!).getAllByText("—")).toHaveLength(3);
  });

  it("labels hydration with the same buckets as the other tables", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    // Three buckets, not a boolean: "hydrating" is a real state, and collapsing
    // it into "not hydrated" would misreport an object that is catching up.
    expect(await screen.findByText("Not Hydrated")).toBeVisible();
    expect(screen.getAllByText("Hydrated").length).toBeGreaterThan(0);
  });

  it("sorts on a column when its header is clicked", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    await userEvent.click(await screen.findByText(/^Object/));
    const names = screen
      .getAllByRole("row")
      .slice(1)
      .map((row) => within(row).getAllByRole("cell")[1].textContent);
    expect(names?.[0]).toContain("legacy_sink");
  });

  it("names an unreadable reading rather than printing Infinity", async () => {
    // `UNREADABLE` is `Infinity`, so the cell is the one place it could reach
    // the screen.
    await renderComponent(
      <FreshnessTable
        rows={[
          buildRow({
            key: "u9",
            objectName: "unreadable_mv",
            current: UNREADABLE,
            peak: UNREADABLE,
            p90: 400,
            breachValue: UNREADABLE,
            breaching: true,
          }),
        ]}
      />,
    );

    const row = (await screen.findByText("unreadable_mv")).closest("tr");
    expect(within(row!).getAllByText(NULL_LAG_TEXT)).toHaveLength(2);
    expect(screen.queryByText(/∞|Infinity/)).not.toBeInTheDocument();
  });

  it("opens sorted worst first", async () => {
    await renderComponent(<FreshnessTable rows={rows} />);

    await screen.findByText("orders_mv");
    const names = screen
      .getAllByRole("row")
      .slice(1)
      .map((row) => within(row).getAllByRole("cell")[1].textContent);
    // The worst peak leads, so the row a reader came for is the first one.
    expect(names[0]).toContain("orders_mv");
  });

  it("pages rather than mounting a row per object", async () => {
    // A cluster can carry thousands of objects, and a `tr` each would be
    // mounted for all of them.
    const many = Array.from({ length: 60 }, (_, i) =>
      buildRow({
        key: `u${i + 10}`,
        objectName: `object_${String(i).padStart(2, "0")}`,
        peak: 1_000 + i,
      }),
    );
    await renderComponent(<FreshnessTable rows={many} />);

    await screen.findByText("object_00");
    // 25 rows plus the header.
    expect(screen.getAllByRole("row")).toHaveLength(26);
    expect(screen.queryByText("object_59")).not.toBeInTheDocument();
  });
});
