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

import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import { SelfManagedNavMenu } from "./NavMenu";

const SELF_MANAGED_REGION_ID = "local/flexible-deployment";

describe("SelfManagedNavMenu", () => {
  it("links to query history", async () => {
    await renderComponent(<SelfManagedNavMenu isMobile={false} />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, SELF_MANAGED_REGION_ID, healthyEnvironment),
    });

    expect(
      (await screen.findByText("Query History")).closest("a"),
    ).toHaveAttribute(
      "href",
      "/regions/local-flexible-deployment/query-history",
    );
  });
});
