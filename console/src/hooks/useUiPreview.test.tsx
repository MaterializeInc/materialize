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

import { UI_PREVIEWS } from "~/config/uiPreviews";
import { useUiPreview } from "~/hooks/useUiPreview";
import { renderComponent } from "~/test/utils";

const clusterListPreview = UI_PREVIEWS.clusterListUsageMetrics;

const flags: Record<string, boolean> = {};

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => flags,
}));

const appConfig = { mode: "cloud" as "cloud" | "self-managed" };

vi.mock("~/config/useAppConfig", () => ({
  useAppConfig: () => appConfig,
}));

const Probe = () => {
  const { isAvailable, isEnabled } = useUiPreview("clusterListUsageMetrics");
  return (
    <>
      <div>available: {String(isAvailable)}</div>
      <div>enabled: {String(isEnabled)}</div>
    </>
  );
};

describe("useUiPreview", () => {
  beforeEach(() => {
    appConfig.mode = "cloud";
    delete flags[clusterListPreview.ldFlag];
    localStorage.clear();
  });

  it("requires the opt-in on top of the flag in cloud mode", async () => {
    flags[clusterListPreview.ldFlag] = true;
    await renderComponent(<Probe />);
    expect(screen.getByText("available: true")).toBeInTheDocument();
    expect(screen.getByText("enabled: false")).toBeInTheDocument();
  });

  it("ships the flagged UI outright in self-managed mode, offering no choice", async () => {
    appConfig.mode = "self-managed";
    flags[clusterListPreview.ldFlag] = true;
    await renderComponent(<Probe />);
    expect(screen.getByText("available: false")).toBeInTheDocument();
    expect(screen.getByText("enabled: true")).toBeInTheDocument();
  });
});
