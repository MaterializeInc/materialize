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

import SqlSelectTable from "./SqlSelectTable";

// Regression test for CNS-30: console treated SELECT FROM (SELECT 1) as having
// empty results because the no-column-names branch always rendered "No results"
// regardless of whether rows were present.
describe("SqlSelectTable — no-column-names branch (CNS-30)", () => {
  it("shows 'No results' when there are no rows and no column names", async () => {
    await renderComponent(
      <SqlSelectTable colNames={[]} paginatedRows={[]} rows={[]} />,
    );
    expect(screen.getByText("No results")).toBeInTheDocument();
  });

  it("shows row count for a single row when column names are absent", async () => {
    // Simulates: SELECT FROM (SELECT 1) — returns 1 row with no named columns
    await renderComponent(
      <SqlSelectTable colNames={[]} paginatedRows={[]} rows={[[]]} />,
    );
    expect(screen.getByText("(1 row)")).toBeInTheDocument();
    expect(screen.queryByText("No results")).not.toBeInTheDocument();
  });

  it("shows plural row count when column names are absent and multiple rows exist", async () => {
    await renderComponent(
      <SqlSelectTable colNames={[]} paginatedRows={[]} rows={[[], []]} />,
    );
    expect(screen.getByText("(2 rows)")).toBeInTheDocument();
    expect(screen.queryByText("No results")).not.toBeInTheDocument();
  });

  it("shows 'No results' when rows prop is undefined", async () => {
    await renderComponent(
      <SqlSelectTable colNames={[]} paginatedRows={[]} />,
    );
    expect(screen.getByText("No results")).toBeInTheDocument();
  });
});
