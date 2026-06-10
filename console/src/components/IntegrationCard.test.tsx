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

import { renderComponent } from "~/test/utils";
import { assert } from "~/util";

import IntegrationCard from "./IntegrationCard";

describe("IntegrationCard", () => {
  it("renders sucessfully for external links", async () => {
    await renderComponent(
      <IntegrationCard
        imagePath="path/to/image.jpg"
        status="Native"
        name="Test"
        description="Test description"
        link="https://external.link.example.com"
      />,
    );
    const card = screen.getByTestId<HTMLAnchorElement>("integration-card");
    expect(card.target).toEqual("_blank");
    const image = card.querySelector("img");
    assert(image);
    expect(image.src).toContain("path/to/image.jpg");

    const action = screen.getByTestId("integration-card-action");
    expect(action).toHaveTextContent("View Integration");
    const externalIcon = action.querySelector("svg");
    assert(externalIcon);
    expect(externalIcon).toBeVisible();
  });

  it("renders sucessfully for coming soon integrations", async () => {
    await renderComponent(
      <IntegrationCard
        imagePath="path/to/image.jpg"
        status="Coming soon"
        name="Test"
        description="Test description"
        link="https://external.link.example.com"
      />,
    );
    const card = screen.getByTestId<HTMLAnchorElement>("integration-card");
    expect(card.target).toEqual("_blank");
    const action = screen.getByTestId("integration-card-action");
    expect(action).toHaveTextContent("Get notified");
    const externalIcon = action.querySelector("svg");
    expect(externalIcon).toBeVisible();
  });

  it("renders sucessfully for source creation links", async () => {
    await renderComponent(
      <IntegrationCard
        imagePath="path/to/image.jpg"
        status="Native"
        name="Test"
        description="Test description"
        link="../path/to/internal/source"
      />,
    );
    const card = screen.getByTestId<HTMLAnchorElement>("integration-card");
    expect(card.target).not.toEqual("_blank");
    const action = screen.getByTestId("integration-card-action");
    expect(action).toHaveTextContent("View Integration");
    const externalIcon = action.querySelector("svg");
    assert(externalIcon === null);
  });
});
