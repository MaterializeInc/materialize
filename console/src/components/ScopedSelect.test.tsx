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

import { renderComponent } from "~/test/utils";

import ScopedSelect from "./ScopedSelect";

const SCOPES = [
  { value: "cluster", label: "Cluster" },
  { value: "schema", label: "Schema" },
] as const;

describe("ScopedSelect", () => {
  it("offers each scope and reports the one picked", async () => {
    const onScopeChange = vi.fn();
    await renderComponent(
      <ScopedSelect
        scopeLabel="Show objects by"
        scope="cluster"
        scopes={[...SCOPES]}
        onScopeChange={onScopeChange}
      >
        <button type="button">pick an item</button>
      </ScopedSelect>,
    );

    const menu = screen.getByRole("combobox", { name: "Show objects by" });
    expect(menu).toHaveValue("cluster");
    expect(
      [...menu.querySelectorAll("option")].map((o) => o.textContent),
    ).toEqual(["Cluster", "Schema"]);

    await userEvent.selectOptions(menu, "schema");
    expect(onScopeChange).toHaveBeenCalledWith("schema");
  });

  it("renders the item select beside the scope menu", async () => {
    await renderComponent(
      <ScopedSelect
        scopeLabel="Show objects by"
        scope="cluster"
        scopes={[...SCOPES]}
        onScopeChange={() => {}}
      >
        <button type="button">pick an item</button>
      </ScopedSelect>,
    );

    expect(screen.getByRole("button", { name: "pick an item" })).toBeVisible();
  });
});
