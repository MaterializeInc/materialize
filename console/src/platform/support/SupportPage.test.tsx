// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { show, showNewMessage } from "@intercom/messenger-js-sdk";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import { STATUSPAGE_URL } from "~/api/incident-io";
import { Incident, StatusSummary } from "~/api/incident-io/types";
import server from "~/api/mocks/server";
import {
  COMMUNITY_SLACK_URL,
  HELP_CENTER_TICKETS_URL,
  HELP_CENTER_URL,
  STATUS_PAGE_URL,
  SUPPORT_PLANS_URL,
} from "~/externalUrls";
import { renderComponent } from "~/test/utils";

import SupportPage, { OUTAGE_MAILTO, SUPPORT_MAILTO } from "./SupportPage";

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  update: vi.fn(),
  show: vi.fn(),
  showNewMessage: vi.fn(),
  onUnreadCountChange: vi.fn((callback: (count: number) => void) =>
    callback(2),
  ),
}));

type IntercomStub = ((...args: unknown[]) => void) & { booted?: boolean };

const setIntercom = (state: "missing" | "stub" | "booted") => {
  const win = window as unknown as { Intercom?: IntercomStub };
  if (state === "missing") {
    delete win.Intercom;
    return;
  }
  const intercom: IntercomStub = vi.fn<(...args: unknown[]) => void>();
  if (state === "booted") intercom.booted = true;
  win.Intercom = intercom;
};

const intercomJwtHandler = (status = 200) =>
  http.post("*/api/organization/intercom/jwt", () =>
    status === 200
      ? HttpResponse.json({ jwt: "test-jwt" })
      : HttpResponse.json({}, { status }),
  );

const buildStatusResponse = (incidents: Incident[] = []) =>
  http.get(STATUSPAGE_URL, () => {
    const response: StatusSummary = {
      page_title: "Materialize Status",
      page_url: "https://mz.example.com",
      ongoing_incidents: incidents,
      in_progress_maintenances: [],
      scheduled_maintenances: [],
    };
    return HttpResponse.json(response);
  });

describe("SupportPage", () => {
  beforeEach(() => {
    server.use(buildStatusResponse(), intercomJwtHandler());
  });

  afterEach(() => {
    setIntercom("missing");
    vi.clearAllMocks();
  });

  it("shows the organization and its ID", async () => {
    await renderComponent(<SupportPage />);

    expect(
      await screen.findByRole("heading", { name: "Help and support" }),
    ).toBeVisible();
    expect(await screen.findByText("Test Organization")).toBeVisible();
    expect(screen.getByTestId("support-org-id")).toHaveTextContent(
      "Org ID org_id",
    );
  });

  it("links to the general ticket portal, Help Center, Slack, and support plans", async () => {
    await renderComponent(<SupportPage />);

    expect(
      await screen.findByRole("link", { name: "View tickets" }),
    ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    expect(
      screen.getByRole("link", { name: "Open Help Center ↗" }),
    ).toHaveAttribute("href", HELP_CENTER_URL);
    expect(
      screen.getByRole("link", { name: "Community Slack ↗" }),
    ).toHaveAttribute("href", COMMUNITY_SLACK_URL);
    expect(
      screen.getByRole("link", { name: "Support plans and response times ↗" }),
    ).toHaveAttribute("href", SUPPORT_PLANS_URL);
    expect(
      screen.getByRole("link", { name: "support@materialize.com" }),
    ).toHaveAttribute("href", SUPPORT_MAILTO);
  });

  it("opens the Messenger for tickets and outages once Intercom has booted", async () => {
    setIntercom("booted");
    const user = userEvent.setup();
    await renderComponent(<SupportPage />);

    await waitFor(() =>
      expect(screen.getByTestId("support-outage-hint")).toHaveAttribute(
        "aria-label",
        expect.stringMatching(/choose “Report Production Outage” in the chat/),
      ),
    );

    await user.click(screen.getByRole("link", { name: "Open a ticket" }));
    await user.click(
      screen.getByRole("link", { name: "Report a production outage" }),
    );
    expect(showNewMessage).toHaveBeenCalledTimes(2);

    await user.click(screen.getByRole("link", { name: "Ask Matty" }));
    expect(show).toHaveBeenCalledTimes(1);
  });

  it("shows the unread reply count from the Messenger", async () => {
    setIntercom("booted");
    await renderComponent(<SupportPage />);

    expect(await screen.findByTestId("support-unread-count")).toHaveTextContent(
      "2 new replies",
    );
  });

  it.each([
    ["the Intercom script is blocked", "stub" as const],
    ["Intercom never loads", "missing" as const],
  ])("falls back to email when %s", async (_, state) => {
    setIntercom(state);
    await renderComponent(<SupportPage />);

    expect(await screen.findByTestId("support-outage-hint")).toHaveAttribute(
      "aria-label",
      expect.stringMatching(/Chat is unavailable/),
    );
    expect(screen.getByRole("link", { name: "Open a ticket" })).toHaveAttribute(
      "href",
      SUPPORT_MAILTO,
    );
    expect(
      screen.getByRole("link", { name: "Report a production outage" }),
    ).toHaveAttribute("href", OUTAGE_MAILTO);
    expect(screen.queryByTestId("support-unread-count")).toBeNull();
  });

  it("does not use an anonymous Messenger before the user is identified", async () => {
    server.use(intercomJwtHandler(500));
    setIntercom("booted");
    const user = userEvent.setup();
    await renderComponent(<SupportPage />);

    const outage = await screen.findByRole("link", {
      name: "Report a production outage",
    });
    // Following the mailto link is fine in jsdom; the Messenger stays closed.
    await user.click(outage);
    expect(showNewMessage).not.toHaveBeenCalled();
  });

  it("shows live status from incident.io", async () => {
    await renderComponent(<SupportPage />);

    expect(
      await screen.findByTestId("support-status-banner"),
    ).toHaveTextContent(/All systems operational/);
  });

  it("shows an ongoing incident in the status banner", async () => {
    server.use(
      buildStatusResponse([
        {
          id: "incident-1",
          url: "https://mz.example.com/incident-1",
          name: "Elevated query latency",
          status: "investigating",
          current_worst_impact: "partial_outage",
          last_update_at: "2026-10-21T00:00:00Z",
          affected_components: [],
          last_update_message: "We are looking into it.",
        },
      ]),
    );
    await renderComponent(<SupportPage />);

    const banner = await screen.findByTestId("support-status-banner");
    expect(banner).toHaveTextContent(/Ongoing incident/);
    expect(banner).toHaveTextContent(/Elevated query latency/);
  });

  it("only links to the status page when the status API fails", async () => {
    let statusRequested = false;
    server.use(
      http.get(STATUSPAGE_URL, () => {
        statusRequested = true;
        return HttpResponse.json({}, { status: 500 });
      }),
    );
    await renderComponent(<SupportPage />);

    await waitFor(() => expect(statusRequested).toBe(true));
    await waitFor(() =>
      expect(screen.getByTestId("support-status-link")).toHaveAttribute(
        "href",
        STATUS_PAGE_URL,
      ),
    );
    expect(screen.queryByText(/All systems operational/)).toBeNull();
  });
});
