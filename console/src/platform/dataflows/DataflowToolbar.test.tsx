// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ThemeProvider } from "@chakra-ui/react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { lightTheme } from "~/theme";

import { DEFAULT_FILTERS, type Filters } from "./dataflowGraph";
import { DataflowToolbar } from "./DataflowToolbar";

function renderToolbar(
  workerCount: number,
  filters: Filters = DEFAULT_FILTERS,
  onFiltersChange: (next: Filters) => void = () => {},
) {
  return render(
    <ThemeProvider theme={lightTheme}>
      <DataflowToolbar
        filters={filters}
        onFiltersChange={onFiltersChange}
        matchCount={0}
        matchIndex={0}
        onJump={() => {}}
        workerCount={workerCount}
        regionExpanded={false}
      />
    </ThemeProvider>,
  );
}

describe("DataflowToolbar", () => {
  it("disables the skew heatmap options on a single-worker replica, where skew is undefined", () => {
    renderToolbar(1);
    for (const label of [
      "Heat: CPU skew",
      "Heat: memory skew",
      "Heat: schedule skew",
    ]) {
      expect(screen.getByRole("option", { name: label })).toBeDisabled();
    }
    // Non-skew heatmap modes still work fine on a single worker.
    expect(screen.getByRole("option", { name: "Heat: elapsed" })).toBeEnabled();
  });

  it("enables the skew heatmap options once there's more than one worker to compare", () => {
    renderToolbar(4);
    for (const label of [
      "Heat: CPU skew",
      "Heat: memory skew",
      "Heat: schedule skew",
    ]) {
      expect(screen.getByRole("option", { name: label })).toBeEnabled();
    }
  });

  it("debounces typing into the filters object", async () => {
    const onFiltersChange = vi.fn();
    renderToolbar(4, DEFAULT_FILTERS, onFiltersChange);
    await userEvent.type(
      screen.getByPlaceholderText("Search operators"),
      "join",
    );
    await waitFor(() =>
      expect(onFiltersChange).toHaveBeenCalledWith({
        ...DEFAULT_FILTERS,
        search: "join",
      }),
    );
  });

  // The caller resets filters when the dataflow or replica changes
  // (DataflowDetailPage). The debounced local input must follow rather than
  // treat the reset as a stale keystroke and push the old search back.
  it("clears the input when the caller resets the search, without pushing it back", async () => {
    const onFiltersChange = vi.fn();
    const { rerender } = renderToolbar(
      4,
      { ...DEFAULT_FILTERS, search: "join" },
      onFiltersChange,
    );
    const input = screen.getByPlaceholderText("Search operators");
    expect(input).toHaveValue("join");

    rerender(
      <ThemeProvider theme={lightTheme}>
        <DataflowToolbar
          filters={DEFAULT_FILTERS}
          onFiltersChange={onFiltersChange}
          matchCount={0}
          matchIndex={0}
          onJump={() => {}}
          workerCount={4}
          regionExpanded={false}
        />
      </ThemeProvider>,
    );

    expect(input).toHaveValue("");
    // Long enough for the 300ms debounce to have fired had it been armed.
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(onFiltersChange).not.toHaveBeenCalled();
  });
});
