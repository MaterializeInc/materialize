// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text } from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { useTruncationTitles } from "./useTruncationTitles";

const LABEL = "a long cluster name";
const OTHER_LABEL = "a long cluster name, but another one";

const Truncated = ({ label = LABEL }: { label?: string }) => {
  useTruncationTitles();
  return <Text noOfLines={1}>{label}</Text>;
};

/** jsdom has no layout, so give the text the widths a narrow cell would. */
const renderWithWidths = (scrollWidth: number, clientWidth: number) => {
  const { rerender } = render(<Truncated />);
  const element = screen.getByText(LABEL);
  Object.defineProperty(element, "scrollWidth", { value: scrollWidth });
  Object.defineProperty(element, "clientWidth", { value: clientWidth });
  return { element, rerender };
};

describe("useTruncationTitles", () => {
  it("titles truncated text while hovered", async () => {
    const user = userEvent.setup();
    const { element } = renderWithWidths(400, 100);

    await user.hover(element);
    expect(element).toHaveAttribute("title", LABEL);
    await user.unhover(element);
    expect(element).not.toHaveAttribute("title");
  });

  it("leaves text that fits alone", async () => {
    const user = userEvent.setup();
    const { element } = renderWithWidths(100, 100);

    await user.hover(element);
    expect(element).not.toHaveAttribute("title");
  });

  it("follows the text when it changes while hovered", async () => {
    const user = userEvent.setup();
    const { element, rerender } = renderWithWidths(400, 100);

    await user.hover(element);
    rerender(<Truncated label={OTHER_LABEL} />);
    await waitFor(() => expect(element).toHaveAttribute("title", OTHER_LABEL));
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
    const { element } = renderWithWidths(400, 100);

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
