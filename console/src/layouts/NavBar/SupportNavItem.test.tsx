// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { onUnreadCountChange } from "@intercom/messenger-js-sdk";
import { screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import React from "react";

import server from "~/api/mocks/server";
import { useFlags } from "~/hooks/useFlags";
import { renderComponent } from "~/test/utils";

import { SupportNavItem } from "./NavMenu";

vi.mock("~/hooks/useFlags", () => ({ useFlags: vi.fn() }));

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
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
    vi.resetAllMocks();
  });

  it("is hidden when the flag is off", async () => {
    setFlag(false);
    await renderComponent(<SupportNavItem />);

    expect(screen.queryByText("Help and support")).toBeNull();
  });

  it("links to the support page without a dot when nothing is unread", async () => {
    setFlag(true);
    vi.mocked(onUnreadCountChange).mockImplementation((callback) =>
      callback(0),
    );
    bootIntercom();
    await renderComponent(<SupportNavItem />);

    expect(
      await screen.findByRole("link", { name: /Help and support/ }),
    ).toHaveAttribute("href", "/support");
    expect(screen.queryByTestId("nav-indicator")).toBeNull();
  });

  it("shows a dot driven by the Messenger's unread count", async () => {
    setFlag(true);
    vi.mocked(onUnreadCountChange).mockImplementation((callback) =>
      callback(1),
    );
    bootIntercom();
    await renderComponent(<SupportNavItem />);

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
    await renderComponent(<SupportNavItem />);

    expect(
      await screen.findByRole("link", { name: /Help and support/ }),
    ).toBeVisible();
    expect(screen.queryByTestId("nav-indicator")).toBeNull();
  });
});
