// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { show, showNewMessage } from "@intercom/messenger-js-sdk";
import { QueryClient } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import { SupportTicket } from "~/api/cloudGlobalApi";
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
import { incidentIOQueryKeys } from "~/queries/incident-io";
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

const TICKETS_URL = "*/api/organization/support/tickets";

const ticketsUnavailableHandler = http.get(TICKETS_URL, () =>
  HttpResponse.json({}, { status: 503 }),
);

const buildTicket = (
  overrides: Partial<SupportTicket> = {},
): SupportTicket => ({
  id: "42",
  number: "4821",
  title: "Replica keeps restarting",
  state: "Waiting on you",
  open: true,
  createdAt: "2026-10-20T12:00:00Z",
  updatedAt: "2026-10-21T12:00:00Z",
  ...overrides,
});

const ticketsHandler = (tickets: SupportTicket[]) =>
  http.get(TICKETS_URL, () =>
    HttpResponse.json({ data: tickets, nextCursor: null }),
  );

describe("SupportPage", () => {
  beforeEach(() => {
    server.use(
      buildStatusResponse(),
      intercomJwtHandler(),
      ticketsUnavailableHandler,
    );
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

  it("stops showing cached status when a refresh fails", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    await renderComponent(<SupportPage />, { queryClient });
    expect(
      await screen.findByTestId("support-status-banner"),
    ).toHaveTextContent(/All systems operational/);

    server.use(
      http.get(STATUSPAGE_URL, () => HttpResponse.json({}, { status: 500 })),
    );
    await queryClient.refetchQueries({ queryKey: incidentIOQueryKeys.all() });

    expect(await screen.findByTestId("support-status-link")).toHaveAttribute(
      "href",
      STATUS_PAGE_URL,
    );
    expect(screen.queryByText(/All systems operational/)).toBeNull();
  });

  it("opens the Messenger if it boots after the page rendered", async () => {
    setIntercom("stub");
    const user = userEvent.setup();
    await renderComponent(<SupportPage />);
    const openTicket = await screen.findByRole("link", {
      name: "Open a ticket",
    });

    // The widget finishes booting between readiness checks.
    (window as unknown as { Intercom: IntercomStub }).Intercom.booted = true;
    await user.click(openTicket);

    expect(showNewMessage).toHaveBeenCalledTimes(1);
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

  describe("with the ticket endpoints", () => {
    it("lists the user's tickets", async () => {
      server.use(
        ticketsHandler([
          buildTicket(),
          buildTicket({
            id: "43",
            number: "4702",
            title: "RBAC for a dbt service account",
            state: "Resolved",
            open: false,
          }),
        ]),
      );
      await renderComponent(<SupportPage />);

      expect(
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(screen.getByText("#4821")).toBeVisible();
      expect(screen.getByText("Waiting on you")).toBeVisible();
      expect(
        screen.getByRole("button", { name: "RBAC for a dbt service account" }),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "All tickets in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
      expect(screen.queryByRole("link", { name: "View tickets" })).toBeNull();
    });

    it("shows an empty state", async () => {
      server.use(ticketsHandler([]));
      await renderComponent(<SupportPage />);

      expect(await screen.findByTestId("support-tickets-empty")).toBeVisible();
    });

    it("shows a ticket's conversation", async () => {
      server.use(
        ticketsHandler([buildTicket()]),
        http.get(`${TICKETS_URL}/42`, () =>
          HttpResponse.json({
            ticket: buildTicket(),
            description: "<p>It restarts every few minutes.</p>",
            messages: [
              {
                id: "m1",
                fromYou: false,
                authorName: "Katrina",
                bodyHtml:
                  "<p>Thanks, looking now.<br>Can you share the cluster?</p>",
                createdAt: "2026-10-20T13:00:00Z",
              },
            ],
          }),
        ),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPage />);

      await user.click(
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
      );

      const ticket = await screen.findByTestId("support-ticket");
      expect(ticket).toHaveTextContent("It restarts every few minutes.");
      expect(ticket).toHaveTextContent("Katrina");
      expect(ticket).toHaveTextContent("Thanks, looking now.");
      expect(
        screen.getByRole("link", { name: "Reply in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    });

    it("opens a ticket from the form", async () => {
      let createdWith: unknown;
      server.use(
        ticketsHandler([]),
        http.post(TICKETS_URL, async ({ request }) => {
          createdWith = await request.json();
          return HttpResponse.json(buildTicket({ title: "Sink is lagging" }), {
            status: 201,
          });
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPage />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      expect(showNewMessage).not.toHaveBeenCalled();

      await user.click(
        await screen.findByRole("button", { name: "Open ticket" }),
      );
      expect(await screen.findByText("A title is required.")).toBeVisible();

      await user.type(screen.getByLabelText("Title"), "Sink is lagging");
      await user.selectOptions(screen.getByLabelText("Severity"), "high");
      await user.type(
        screen.getByLabelText("What's happening?"),
        "Kafka sink is 10 minutes behind.",
      );
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      await waitFor(() =>
        expect(createdWith).toMatchObject({
          title: "Sink is lagging",
          description: "Kafka sink is 10 minutes behind.",
          severity: "high",
          environmentType: "production",
        }),
      );
      expect(await screen.findByText(/Ticket #4821 opened/)).toBeVisible();
    });

    it("keeps the form open with an error when creating fails", async () => {
      server.use(
        ticketsHandler([]),
        http.post(TICKETS_URL, () => HttpResponse.json({}, { status: 502 })),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPage />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.type(await screen.findByLabelText("Title"), "Sink is lagging");
      await user.type(
        screen.getByLabelText("What's happening?"),
        "Kafka sink is 10 minutes behind.",
      );
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      expect(
        await screen.findByText(/Your ticket couldn't be opened/),
      ).toBeVisible();
      expect(screen.getByLabelText("Title")).toHaveValue("Sink is lagging");
    });
  });
});
