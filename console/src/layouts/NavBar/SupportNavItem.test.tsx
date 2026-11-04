// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { onUnreadCountChange } from "@intercom/messenger-js-sdk";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import server from "~/api/mocks/server";
import { useFlags } from "~/hooks/useFlags";
import { resetMessengerStateForTests, useIntercom } from "~/hooks/useIntercom";
import { renderComponent } from "~/test/utils";

import { SupportNavItem } from "./NavMenu";

vi.mock("~/hooks/useFlags", () => ({ useFlags: vi.fn() }));

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
  onShow: vi.fn(),
  onHide: vi.fn(),
  onUnreadCountChange: vi.fn(),
}));

const setFlag = (enabled: boolean) =>
  vi
    .mocked(useFlags)
    .mockReturnValue({ "support-page-cns-147": enabled } as ReturnType<
      typeof useFlags
    >);

const bootIntercom = () => {
  const intercom = Object.assign(vi.fn(), { booted: true });
  (window as unknown as { Intercom?: unknown }).Intercom = intercom;
};

/** The nav item with the Messenger booted for the user, as the app does. */
const SupportNavItemWithIntercom = () => {
  useIntercom();
  return <SupportNavItem />;
};

describe("SupportNavItem", () => {
  beforeEach(() => {
    server.use(
      http.post("*/api/organization/intercom/jwt", () =>
        HttpResponse.json({ jwt: "test-jwt" }),
      ),
    );
  });

  afterEach(() => {
    delete (window as unknown as { Intercom?: unknown }).Intercom;
    resetMessengerStateForTests();
    vi.resetAllMocks();
  });

  it("is hidden when the flag is off", async () => {
    setFlag(false);
    await renderComponent(<SupportNavItemWithIntercom />);

    expect(screen.queryByText("Help and support")).toBeNull();
  });

  it("links to the support page without a dot when nothing is unread", async () => {
    setFlag(true);
    vi.mocked(onUnreadCountChange).mockImplementation((callback) =>
      callback(0),
    );
    bootIntercom();
    await renderComponent(<SupportNavItemWithIntercom />);

    expect(
      await screen.findByRole("link", { name: /Help and support/ }),
    ).toHaveAttribute("href", "/support");
    expect(screen.queryByTestId("nav-indicator")).toBeNull();
  });

  it("starts loading the tickets on hover", async () => {
    setFlag(true);
    let ticketRequests = 0;
    server.use(
      http.get("*/api/organization/support/tickets", () => {
        ticketRequests += 1;
        return HttpResponse.json({ data: [], nextCursor: null });
      }),
    );
    const user = userEvent.setup();
    await renderComponent(<SupportNavItemWithIntercom />);
    const link = await screen.findByRole("link", { name: /Help and support/ });
    expect(ticketRequests).toBe(0);

    await user.hover(link);

    await waitFor(() => expect(ticketRequests).toBe(1));
  });

  it("shows a dot driven by the Messenger's unread count", async () => {
    setFlag(true);
    vi.mocked(onUnreadCountChange).mockImplementation((callback) =>
      callback(1),
    );
    bootIntercom();
    await renderComponent(<SupportNavItemWithIntercom />);

    expect(await screen.findByTestId("nav-indicator")).toHaveAttribute(
      "aria-label",
      "1 unread support reply",
    );
  });

  it("shows no dot when Intercom has not booted", async () => {
    setFlag(true);
    vi.mocked(onUnreadCountChange).mockImplementation((callback) =>
      callback(3),
    );
    await renderComponent(<SupportNavItemWithIntercom />);

    expect(
      await screen.findByRole("link", { name: /Help and support/ }),
    ).toBeVisible();
    expect(screen.queryByTestId("nav-indicator")).toBeNull();
  });
});
