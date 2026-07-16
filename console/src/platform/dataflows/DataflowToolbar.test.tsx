// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ThemeProvider } from "@chakra-ui/react";
import { render, screen } from "@testing-library/react";
import React from "react";

import { lightTheme } from "~/theme";

import { DEFAULT_FILTERS } from "./dataflowGraph";
import { DataflowToolbar } from "./DataflowToolbar";

function renderToolbar(workerCount: number) {
  return render(
    <ThemeProvider theme={lightTheme}>
      <DataflowToolbar
        filters={DEFAULT_FILTERS}
        onFiltersChange={() => {}}
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
});
