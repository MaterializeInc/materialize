// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, Tooltip } from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { registerTruncationTitles } from "./truncationTitles";

const LABEL = "a long cluster name";
const QUALIFIED_LABEL = `materialize.public.${LABEL}`;

/** jsdom has no layout, so give the text the widths a narrow cell would. */
const renderWithWidths = (
  scrollWidth: number,
  clientWidth: number,
  ui = <Text noOfLines={1}>{LABEL}</Text>,
) => {
  render(ui);
  const element = screen.getByText(LABEL);
  Object.defineProperty(element, "scrollWidth", { value: scrollWidth });
  Object.defineProperty(element, "clientWidth", { value: clientWidth });
  return element;
};

describe("registerTruncationTitles", () => {
  let unregister: () => void;

  beforeEach(() => {
    unregister = registerTruncationTitles();
  });

  afterEach(() => {
    unregister();
  });

  it("titles truncated text while hovered", async () => {
    const user = userEvent.setup();
    const element = renderWithWidths(400, 100);

    await user.hover(element);
    expect(element).toHaveAttribute("title", LABEL);
    await user.unhover(element);
    expect(element).not.toHaveAttribute("title");
  });

  it("leaves text that fits alone", async () => {
    const user = userEvent.setup();
    const element = renderWithWidths(100, 100);

    await user.hover(element);
    expect(element).not.toHaveAttribute("title");
  });

  it("titles truncated text when an inline child is hovered", async () => {
    const user = userEvent.setup();
    const element = renderWithWidths(
      400,
      100,
      <Text noOfLines={1}>
        {LABEL} <b>in bold</b>
      </Text>,
    );

    await user.hover(screen.getByText("in bold"));
    expect(element).toHaveAttribute("title", `${LABEL} in bold`);
  });

  it("leaves truncated text alone where a tooltip shows it in full", async () => {
    const user = userEvent.setup();
    const element = renderWithWidths(
      400,
      100,
      <Tooltip label={QUALIFIED_LABEL}>
        <Box data-no-truncation-title>
          <Text noOfLines={1}>{LABEL}</Text>
        </Box>
      </Tooltip>,
    );

    await user.hover(element);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      QUALIFIED_LABEL,
    );
    expect(element).not.toHaveAttribute("title");
  });

  it("keeps the text out of Sentry click breadcrumbs", async () => {
    // Without a DSN, Sentry sets up no integrations and records no breadcrumbs.
    Sentry.init({
      dsn: "https://key@o0.ingest.sentry.io/0",
      defaultIntegrations: false,
      integrations: [Sentry.breadcrumbsIntegration()],
      transport: () => ({ send: async () => ({}), flush: async () => true }),
    });
    const user = userEvent.setup();
    const element = renderWithWidths(400, 100);

    await user.click(element);
    expect(element).toHaveAttribute("title", LABEL);
    const { breadcrumbs } = Sentry.getIsolationScope().getScopeData();
    expect(
      breadcrumbs
        .filter((b) => b.category === "ui.click")
        .map((b) => b.message),
    ).toEqual(["body > div > p"]);
  });
});
