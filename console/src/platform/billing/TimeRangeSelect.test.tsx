// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { renderComponent, selectReactSelectOption } from "~/test/utils";

import TimeRangeSelect from "./TimeRangeSelect";

describe("TimeRangeSelect", () => {
  it("offers Last 60 days between Last 30 days and Last 90 days", async () => {
    await renderComponent(
      <TimeRangeSelect timeRange={7} setTimeRange={vi.fn()} />,
    );
    await userEvent.click(screen.getByLabelText("Select a time range"));
    const options = screen.getAllByRole("option").map((el) => el.textContent);
    expect(options).toEqual([
      "Last 7 days",
      "Last 14 days",
      "Last 30 days",
      "Last 60 days",
      "Last 90 days",
      "Last 100 days",
    ]);
  });

  it("calls setTimeRange with 60 when Last 60 days is selected", async () => {
    const setTimeRange = vi.fn();
    const { container } = await renderComponent(
      <TimeRangeSelect timeRange={7} setTimeRange={setTimeRange} />,
    );
    await selectReactSelectOption(container, "Last 60 days");
    expect(setTimeRange).toHaveBeenCalledWith(60);
  });
});
