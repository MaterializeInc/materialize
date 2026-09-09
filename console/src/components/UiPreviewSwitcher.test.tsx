// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Menu, MenuList } from "@chakra-ui/react";
import { fireEvent, screen } from "@testing-library/react";
import React from "react";

import { UI_PREVIEWS } from "~/config/uiPreviews";
import { uiPreviewOptInStorageKey } from "~/hooks/useUiPreview";
import { renderComponent } from "~/test/utils";

import UiPreviewSwitcher from "./UiPreviewSwitcher";

const clusterListPreview = UI_PREVIEWS.clusterListUsageMetrics;

const flags: Record<string, boolean> = {};

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => flags,
}));

const renderSwitcher = () =>
  renderComponent(
    <Menu isOpen>
      <MenuList>
        <UiPreviewSwitcher />
      </MenuList>
    </Menu>,
  );

describe("UiPreviewSwitcher", () => {
  beforeEach(() => {
    delete flags[clusterListPreview.ldFlag];
    localStorage.clear();
  });

  it("renders nothing when no preview flag is on", async () => {
    await renderSwitcher();
    expect(screen.queryByText("UI previews")).not.toBeInTheDocument();
  });

  it("toggles the opt-in when the preview flag is on", async () => {
    flags[clusterListPreview.ldFlag] = true;
    await renderSwitcher();
    const item = screen.getByRole("menuitemcheckbox", {
      name: clusterListPreview.label,
    });
    expect(item).toHaveAttribute("aria-checked", "false");
    fireEvent.click(item);
    expect(item).toHaveAttribute("aria-checked", "true");
  });

  it("restores a persisted opt-in", async () => {
    flags[clusterListPreview.ldFlag] = true;
    localStorage.setItem(
      uiPreviewOptInStorageKey("clusterListUsageMetrics"),
      "true",
    );
    await renderSwitcher();
    expect(
      screen.getByRole("menuitemcheckbox", { name: clusterListPreview.label }),
    ).toHaveAttribute("aria-checked", "true");
  });
});
