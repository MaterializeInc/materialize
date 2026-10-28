// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen } from "@testing-library/react";
import React from "react";

import { renderComponent } from "~/test/utils";

import BreadcrumbPicker, { BreadcrumbPickerItem } from "./BreadcrumbPicker";

const ITEMS: BreadcrumbPickerItem[] = [
  { id: "quickstart", label: "quickstart" },
  {
    id: "mz_catalog_server",
    label: "mz_catalog_server",
    group: "System clusters",
    searchOnly: true,
  },
  {
    id: "mz_probe",
    label: "mz_probe",
    group: "System clusters",
    searchOnly: true,
  },
];

const openPicker = async (props: { selectedId?: string } = {}) => {
  const onSelect = vi.fn();
  await renderComponent(
    <BreadcrumbPicker
      trigger="Select cluster"
      ariaLabel="Cluster"
      items={ITEMS}
      onSelect={onSelect}
      searchPlaceholder="Search clusters…"
      {...props}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "Cluster" }));
  await screen.findByPlaceholderText("Search clusters…");
  return { onSelect };
};

describe("BreadcrumbPicker", () => {
  it("hides search-only items until the user searches", async () => {
    await openPicker();
    expect(screen.getByText("quickstart")).toBeVisible();
    expect(screen.queryByText("mz_catalog_server")).not.toBeInTheDocument();
    expect(screen.queryByText("System clusters")).not.toBeInTheDocument();
  });

  it("lets the user search for and select a search-only item", async () => {
    const { onSelect } = await openPicker();
    fireEvent.change(screen.getByPlaceholderText("Search clusters…"), {
      target: { value: "catalog" },
    });
    expect(screen.getByText("System clusters")).toBeInTheDocument();
    expect(screen.queryByText("mz_probe")).not.toBeInTheDocument();
    fireEvent.click(screen.getByText("mz_catalog_server"));
    expect(onSelect).toHaveBeenCalledWith(ITEMS[1]);
  });

  it("keeps a selected search-only item visible without a search", async () => {
    await openPicker({ selectedId: "mz_catalog_server" });
    expect(screen.getByText("mz_catalog_server")).toBeVisible();
    expect(screen.queryByText("mz_probe")).not.toBeInTheDocument();
  });
});
