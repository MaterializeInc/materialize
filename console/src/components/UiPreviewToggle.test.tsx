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

import { UI_PREVIEWS } from "~/config/uiPreviews";
import {
  uiPreviewCollapsedStorageKey,
  uiPreviewOptInStorageKey,
} from "~/hooks/useUiPreview";
import { renderComponent } from "~/test/utils";

import { UiPreviewToggle } from "./UiPreviewToggle";

const clusterListPreview = UI_PREVIEWS.clusterListUsageMetrics;

const flags: Record<string, boolean> = {};

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => flags,
}));

const renderToggle = () =>
  renderComponent(
    <UiPreviewToggle
      previewKey="clusterListUsageMetrics"
      label="Try the new cluster list experience"
    />,
  );

describe("UiPreviewToggle", () => {
  beforeEach(() => {
    delete flags[clusterListPreview.ldFlag];
    localStorage.clear();
    sessionStorage.clear();
  });

  it("renders nothing when the preview flag is off", async () => {
    await renderToggle();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("offers the way back after opting in", async () => {
    flags[clusterListPreview.ldFlag] = true;
    await renderToggle();
    const toggle = screen.getByRole("button", {
      name: "Try the new cluster list experience",
    });
    fireEvent.click(toggle);
    expect(toggle).toHaveTextContent("Switch to classic UI");
    expect(toggle).toHaveTextContent("Preview");
  });

  it("restores a persisted opt-in", async () => {
    flags[clusterListPreview.ldFlag] = true;
    localStorage.setItem(
      uiPreviewOptInStorageKey("clusterListUsageMetrics"),
      "true",
    );
    await renderToggle();
    expect(
      screen.getByRole("button", { name: /Switch to classic UI/ }),
    ).toBeInTheDocument();
  });

  it("collapses to a sparkle and expands back, without opting in", async () => {
    flags[clusterListPreview.ldFlag] = true;
    await renderToggle();
    fireEvent.click(screen.getByRole("button", { name: "Collapse preview" }));
    expect(
      screen.queryByText("Try the new cluster list experience"),
    ).not.toBeInTheDocument();
    expect(
      localStorage.getItem(uiPreviewOptInStorageKey("clusterListUsageMetrics")),
    ).not.toBe("true");
    const expand = screen.getByRole("button", { name: "Expand preview" });
    expect(expand).toHaveFocus();
    fireEvent.click(expand);
    expect(
      screen.getByRole("button", {
        name: "Try the new cluster list experience",
      }),
    ).toHaveFocus();
  });

  it("restores a collapse persisted for the session", async () => {
    flags[clusterListPreview.ldFlag] = true;
    sessionStorage.setItem(
      uiPreviewCollapsedStorageKey("clusterListUsageMetrics"),
      "true",
    );
    await renderToggle();
    expect(
      screen.getByRole("button", { name: "Expand preview" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("Try the new cluster list experience"),
    ).not.toBeInTheDocument();
  });
});
