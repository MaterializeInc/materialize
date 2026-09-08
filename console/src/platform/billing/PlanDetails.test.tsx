// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import React, { ReactElement } from "react";

import {
  buildCloudRegionsReponse,
  buildCreditsResponse,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";

import { UpgradedPlanDetails } from "./PlanDetails";

const Wrapper = await createProviderWrapper({
  initializeState: ({ set }) =>
    setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
});

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

describe("UpgradedPlanDetails", () => {
  beforeEach(() => {
    server.use(buildCloudRegionsReponse(), buildCreditsResponse());
  });

  it("renders successfully", async () => {
    renderComponent(<UpgradedPlanDetails />);

    expect(await screen.findByText("Capacity")).toBeVisible();
    expect(await screen.findByText("Total balance")).toBeVisible();
  });
});
