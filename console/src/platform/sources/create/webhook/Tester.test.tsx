// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import React from "react";

import type { Source } from "~/api/materialize/source/sourceList";
import { renderComponent } from "~/test/utils";
import { assert } from "~/util";

import Tester, { Parse } from "./Tester";

const MOCK_SOURCE: Source = {
  id: "u123",
  name: "test_source",
  size: null,
  type: "webhook",
  isOwner: true,
  error: null,
  status: "running",
  schemaName: "public",
  databaseName: "materialize",
  snapshotCommitted: true,
  createdAt: new Date(),
  clusterId: "u1",
  clusterName: "test_cluster",
  connectionId: null,
  connectionName: null,
  kafkaTopic: null,
  webhookUrl:
    "https://environment.materialize.test/cluster/database/schema/source",
};

vi.mock("~/api/materialize/source/useWebhookSourceEvents", () => ({
  __esModule: true,
  default: vi.fn(() => ({
    data: [],
    errors: [],
    isError: false,
  })),
}));

describe("Webhook Tester", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("copying the webhook URL advances the stepper", async () => {
    await renderComponent(
      <Tester
        source={MOCK_SOURCE}
        bodyFormat="bytes"
        viewDdl={null}
        onViewUpdate={(_v, _s) => {}}
        headerColumns={[]}
      />,
    );
    expect(
      screen.getByText("Configure the webhook in your external application"),
    ).toBeVisible();
    const stepper = screen.getByTestId("tester-stepper");
    // Ensure the first step is initally active.
    let activeIndicator = stepper.querySelector(
      "[data-testid^=tester-step-][data-status=active]",
    );
    assert(activeIndicator !== null);
    expect(activeIndicator).toHaveTextContent("1");
    const copyButton = screen.getByTestId("copyable");
    expect(copyButton).toBeVisible();
    await userEvent.click(copyButton);
    // Clicking the copy icon should cause the second step to become active.
    activeIndicator = stepper.querySelector(
      "[data-testid^=tester-step-][data-status=active]",
    );
    assert(activeIndicator !== null);
    expect(activeIndicator).toHaveTextContent("2");
  });

  it("the Websocket poller can be skipped", async () => {
    await renderComponent(
      <Tester
        source={MOCK_SOURCE}
        bodyFormat="bytes"
        viewDdl={null}
        onViewUpdate={(_v, _s) => {}}
        headerColumns={[]}
      />,
    );
    expect(
      screen.getByText("Configure the webhook in your external application"),
    ).toBeVisible();
    const copyButton = screen.getByTestId("copyable");
    await userEvent.click(copyButton);
    // Clicking the skip button should display the skip message and mark the second step as complete.
    const skipButton = screen.getByTestId("spy-skip-button");
    expect(skipButton).toBeVisible();
    await userEvent.click(skipButton);
    expect(skipButton).toHaveTextContent("Skipped");
    const secondStep = screen.getByTestId("tester-step-1");
    const completeIndicator = secondStep.querySelector(
      "[data-status=complete]",
    );
    assert(completeIndicator !== null);
    expect(completeIndicator).toHaveTextContent("");
  });

  it("the view creation step is rendered for JSON sources", async () => {
    await renderComponent(
      <Tester
        source={MOCK_SOURCE}
        bodyFormat="json"
        viewDdl={null}
        onViewUpdate={(_v, _s) => {}}
        headerColumns={[]}
      />,
    );
    expect(screen.getByText("Parse JSON payload")).toBeVisible();
  });

  it("the view creation step is not rendered for byte sources", async () => {
    await renderComponent(
      <Tester
        source={MOCK_SOURCE}
        bodyFormat="bytes"
        viewDdl={null}
        onViewUpdate={(_v, _s) => {}}
        headerColumns={[]}
      />,
    );
    expect(screen.queryByText("Parse JSON payload")).not.toBeInTheDocument();
  });

  it("view creation can be bypassed for JSON sources", async () => {
    await renderComponent(
      <Parse
        source={MOCK_SOURCE}
        bodyFormat="json"
        sampleObject={{ key: "value" }}
        viewDdl={null}
        onViewUpdate={(_v, _s) => {}}
        headerColumns={[]}
        setSampleObject={(_) => {}}
        step={2}
        isActiveStep={(_) => true}
        getStatus={(_) => "active"}
        activeStep={2}
        isIncompleteStep={(_) => true}
        isCompleteStep={(_) => false}
        setActiveStep={(_) => {}}
        goToNext={() => {}}
        goToPrevious={() => {}}
        activeStepPercent={100}
      />,
    );
    expect(screen.getByPlaceholderText("view_name")).toBeVisible();
    const radioGroup = screen.getByTestId("should-parse");
    const noOption = radioGroup.querySelector("[value=no]");
    assert(noOption !== null);
    await userEvent.click(noOption);
    expect(screen.queryByPlaceholderText("view_name")).not.toBeInTheDocument();
  });
});
