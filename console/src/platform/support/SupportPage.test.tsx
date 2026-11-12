// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { show, showNewMessage } from "@intercom/messenger-js-sdk";
import { focusManager, QueryClient } from "@tanstack/react-query";
import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { SupportTicket, SupportTicketDetail } from "~/api/cloudGlobalApi";
import { STATUSPAGE_URL } from "~/api/incident-io";
import { Incident, StatusSummary } from "~/api/incident-io/types";
import server from "~/api/mocks/server";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import {
  HELP_CENTER_TICKETS_URL,
  HELP_CENTER_URL,
  MZ_DEBUG_SELF_MANAGED_URL,
  STATUS_PAGE_URL,
  SUPPORT_CHAT_URL,
  SUPPORT_PLANS_URL,
} from "~/externalUrls";
import { resetMessengerStateForTests, useIntercom } from "~/hooks/useIntercom";
import { incidentIOQueryKeys } from "~/queries/incident-io";
import { defaultRegionId, renderComponent } from "~/test/utils";

import {
  isAttachUnconfirmedError,
  retryWhenUploadsAreBusy,
  supportTicketQueryKeys,
  supportTicketsQueryOptions,
} from "./queries";
import SupportPage, { OUTAGE_MAILTO, SUPPORT_MAILTO } from "./SupportPage";
import {
  ATTACHMENT_LIMITS,
  OUTAGE_TEMPLATE,
  TICKET_TEMPLATE,
} from "./ticketForm";

const mockFlags: Record<string, boolean> = vi.hoisted(() => ({}));

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => mockFlags,
}));

// The Messenger reports itself shown whenever it's asked to open.
const messengerCallbacks = vi.hoisted(() => ({
  onShow: undefined as undefined | (() => void),
}));

vi.mock("@intercom/messenger-js-sdk", () => ({
  Intercom: vi.fn(),
  boot: vi.fn(),
  shutdown: vi.fn(),
  // The running session's user, while a stubbed Messenger is booted.
  whoami: vi.fn(() =>
    (window as unknown as { Intercom?: { booted?: boolean } }).Intercom?.booted
      ? { user_id: "user@example.com", email: "user@example.com" }
      : undefined,
  ),
  update: vi.fn(),
  show: vi.fn(() => messengerCallbacks.onShow?.()),
  showNewMessage: vi.fn(() => messengerCallbacks.onShow?.()),
  onShow: vi.fn((callback: () => void) => {
    messengerCallbacks.onShow = callback;
  }),
  onHide: vi.fn(),
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

/** An Intercom JWT for the user the `whoami` mock reports. */
const TEST_JWT = [
  "header",
  window.btoa(
    JSON.stringify({ user_id: "user@example.com", email: "user@example.com" }),
  ),
  "sig",
].join(".");

const intercomJwtHandler = (status = 200) =>
  http.post("*/api/organization/intercom/jwt", () =>
    status === 200
      ? HttpResponse.json({ jwt: TEST_JWT })
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
const ORGANIZATIONS_URL = "*/api/organization/support/organizations";

/**
 * The support page with the Messenger booted for the signed-in user, as the
 * app's initializer does.
 */
const SupportPageWithIntercom = () => {
  useIntercom();
  return <SupportPage />;
};

/** The browser's Back button. */
const BackButton = () => {
  const navigate = useNavigate();
  return <button onClick={() => navigate(-1)}>Back</button>;
};

/** Shows the current path, to check where the page navigated. */
const CurrentPath = () => (
  <span data-testid="current-path">{useLocation().pathname}</span>
);

const ticketsUnavailableHandler = http.get(TICKETS_URL, () =>
  HttpResponse.json({}, { status: 503 }),
);

/** The global API before it lists support organizations. */
const organizationsUnavailableHandler = http.get(ORGANIZATIONS_URL, () =>
  HttpResponse.json({}, { status: 404 }),
);

const buildTicket = (
  overrides: Partial<SupportTicket> = {},
): SupportTicket => ({
  id: "42",
  number: "4821",
  title: "Replica keeps restarting",
  state: "Waiting on you",
  stateCategory: "waiting_on_customer",
  open: true,
  createdAt: "2026-10-20T12:00:00Z",
  updatedAt: "2026-10-21T12:00:00Z",
  ...overrides,
});

/** A ticket's conversation, for any ticket ID. */
const ticketDetailHandler = (overrides: Partial<SupportTicketDetail> = {}) =>
  http.get(`${TICKETS_URL}/:ticketId`, () =>
    HttpResponse.json({
      ticket: buildTicket(),
      description: "<p>It restarts every few minutes.</p>",
      messages: [],
      ...overrides,
    }),
  );

/** Fills in the ticket form's required fields. */
const fillTicketForm = async (
  user: ReturnType<typeof userEvent.setup>,
  {
    title = "Sink is lagging",
    description = "Kafka sink is 10 minutes behind.",
  }: { title?: string; description?: string } = {},
) => {
  await user.type(
    await screen.findByRole("textbox", { name: /Subject/ }),
    title,
  );
  const details = screen.getByRole("textbox", { name: /What happened/ });
  await user.type(details, description);
};

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
      organizationsUnavailableHandler,
    );
  });

  afterEach(() => {
    focusManager.setFocused(undefined);
    setIntercom("missing");
    resetMessengerStateForTests();
    vi.clearAllMocks();
  });

  // The ticket list can span organizations, so the page doesn't name one.
  it("doesn't show the organization in the header", async () => {
    await renderComponent(<SupportPageWithIntercom />);

    expect(
      await screen.findByRole("heading", { name: "Help and support" }),
    ).toBeVisible();
    expect(screen.queryByText("Test Organization")).not.toBeInTheDocument();
    expect(screen.queryByText(/Org ID/)).not.toBeInTheDocument();
  });

  it("links to the general ticket portal, Help Center, Slack, and support plans", async () => {
    await renderComponent(<SupportPageWithIntercom />);

    expect(
      await screen.findByRole("link", { name: "View tickets" }),
    ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    expect(
      screen.getByRole("link", { name: "Open Help Center ↗" }),
    ).toHaveAttribute("href", HELP_CENTER_URL);
    expect(
      screen.getByRole("link", { name: "Community Slack ↗" }),
    ).toHaveAttribute("href", SUPPORT_CHAT_URL);
    expect(
      screen.getByRole("link", { name: "Support plans and response times ↗" }),
    ).toHaveAttribute("href", SUPPORT_PLANS_URL);
    expect(
      screen.getByRole("link", { name: "support@materialize.com" }),
    ).toHaveAttribute("href", SUPPORT_MAILTO);
  });

  it("shows quick answers in the sidebar and support plans under the ticket buttons", async () => {
    await renderComponent(<SupportPageWithIntercom />);

    const sidebar = await screen.findByRole("complementary", {
      name: "More ways to get help",
    });
    expect(
      within(sidebar).getByRole("region", { name: "Get answers now" }),
    ).toBeVisible();
    // The form explains severities, so the page doesn't repeat them.
    expect(
      screen.queryByRole("region", { name: "Before you open a ticket" }),
    ).not.toBeInTheDocument();
    expect(
      within(screen.getByTestId("support-ticket-actions")).getByRole("link", {
        name: "Support plans and response times ↗",
      }),
    ).toHaveAttribute("href", SUPPORT_PLANS_URL);
  });

  it("opens the Messenger for tickets and outages once Intercom has booted", async () => {
    setIntercom("booted");
    const user = userEvent.setup();
    await renderComponent(<SupportPageWithIntercom />);

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
    await renderComponent(<SupportPageWithIntercom />);

    expect(await screen.findByTestId("support-unread-count")).toHaveTextContent(
      "2 new replies",
    );
  });

  it.each([
    ["the Intercom script is blocked", "stub" as const],
    ["Intercom never loads", "missing" as const],
  ])("falls back to email when %s", async (_, state) => {
    setIntercom(state);
    await renderComponent(<SupportPageWithIntercom />);

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

  it("does not use a Messenger booted anonymously", async () => {
    // e.g. the Messenger booted before sign-in.
    setIntercom("booted");
    const user = userEvent.setup();
    await renderComponent(<SupportPage />);

    const outage = await screen.findByRole("link", {
      name: "Report a production outage",
    });
    await user.click(outage);
    expect(showNewMessage).not.toHaveBeenCalled();
    expect(screen.getByTestId("support-outage-hint")).toHaveAttribute(
      "aria-label",
      "Chat is unavailable, so these open an email to support.",
    );
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
    await renderComponent(<SupportPageWithIntercom />);

    const banner = await screen.findByTestId("support-status-banner");
    expect(banner).toHaveTextContent(/Ongoing incident/);
    expect(banner).toHaveTextContent(/Elevated query latency/);
  });

  it("stops showing cached status when a refresh fails", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, retryDelay: 0 } },
    });
    await renderComponent(<SupportPageWithIntercom />, { queryClient });
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
    await renderComponent(<SupportPageWithIntercom />);
    const openTicket = await screen.findByRole("link", {
      name: "Open a ticket",
    });

    // The widget finishes booting between readiness checks.
    (window as unknown as { Intercom: IntercomStub }).Intercom.booted = true;
    await user.click(openTicket);

    expect(showNewMessage).toHaveBeenCalledTimes(1);
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
            stateCategory: "resolved",
            open: false,
          }),
        ]),
      );
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(screen.getByText("#4821")).toBeVisible();
      expect(screen.getByText("Waiting on you")).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toHaveAttribute("href", "/support/tickets/42");
      // Only open tickets show until another status is picked.
      expect(screen.getByRole("radio", { name: "Open" })).toBeChecked();
      expect(
        screen.queryByRole("link", { name: "RBAC for a dbt service account" }),
      ).not.toBeInTheDocument();
      expect(
        screen.getByRole("link", { name: "View in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
      expect(screen.queryByRole("link", { name: "View tickets" })).toBeNull();
    });

    it("loads every page of tickets", async () => {
      const limits: (string | null)[] = [];
      server.use(
        http.get(TICKETS_URL, ({ request }) => {
          const params = new URL(request.url).searchParams;
          limits.push(params.get("limit"));
          return HttpResponse.json(
            params.get("cursor") === "page-2"
              ? {
                  data: [buildTicket({ id: "7", title: "Sink is lagging" })],
                  nextCursor: null,
                }
              : { data: [buildTicket()], nextCursor: "page-2" },
          );
        }),
      );
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(limits).toEqual(["10", "50"]);
      expect(
        screen.queryByRole("button", { name: "Load more" }),
      ).not.toBeInTheDocument();
    });

    it("shows each ticket's organization and who opened it", async () => {
      server.use(
        ticketsHandler([
          // Organizations the user belongs to show by name.
          buildTicket({
            organizationId: "tenant-id",
            openedBy: "USER@example.com",
          }),
          buildTicket({
            id: "7",
            number: "4822",
            title: "Sink is lagging",
            organizationId: "9e8b1bc8-fa08-4870-a21f-22ac01cc3808",
            openedBy: "teammate@example.com",
          }),
          // Tickets opened in the Messenger have neither.
          buildTicket({ id: "8", number: "4823", title: "From the Messenger" }),
        ]),
      );
      await renderComponent(<SupportPageWithIntercom />);

      const table = await screen.findByTestId("support-tickets-table");
      expect(
        within(table).getByRole("columnheader", { name: /Organization/ }),
      ).toBeVisible();
      expect(
        within(table).getByRole("columnheader", { name: /Opened by/ }),
      ).toBeVisible();
      const row = (title: string) =>
        within(table).getByRole("link", { name: title }).closest("tr")!;
      expect(row("Replica keeps restarting")).toHaveTextContent("tenant-name");
      expect(row("Replica keeps restarting")).toHaveTextContent("You");
      // Other organizations show the start of their ID, with all of it on hover.
      expect(
        within(row("Sink is lagging")).getByText("9e8b1bc8"),
      ).toHaveAttribute("title", "9e8b1bc8-fa08-4870-a21f-22ac01cc3808");
      expect(row("Sink is lagging")).toHaveTextContent("teammate@example.com");
      expect(within(row("From the Messenger")).getAllByText("—")).toHaveLength(
        2,
      );
    });

    it("filters tickets by organization", async () => {
      server.use(
        ticketsHandler([
          buildTicket({ organizationId: "tenant-id" }),
          buildTicket({
            id: "7",
            number: "4822",
            title: "Sink is lagging",
            organizationId: "9e8b1bc8-fa08-4870-a21f-22ac01cc3808",
          }),
          buildTicket({ id: "8", number: "4823", title: "From the Messenger" }),
        ]),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      const filter = await screen.findByRole("combobox", {
        name: "Filter tickets by organization",
      });
      expect(filter).toHaveValue("");
      // Organizations the user isn't in show the start of their ID.
      await user.selectOptions(filter, "9e8b1bc8");
      expect(
        screen.getByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Replica keeps restarting" }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole("link", { name: "From the Messenger" }),
      ).not.toBeInTheDocument();

      await user.selectOptions(filter, "tenant-name");
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();

      await user.selectOptions(filter, "All organizations");
      expect(
        screen.getByRole("link", { name: "From the Messenger" }),
      ).toBeVisible();
    });

    it("keeps a selected organization's filter clearable after its tickets go", async () => {
      const sinkTicket = buildTicket({
        id: "7",
        number: "4822",
        title: "Sink is lagging",
        organizationId: "9e8b1bc8-fa08-4870-a21f-22ac01cc3808",
      });
      server.use(
        ticketsHandler([
          buildTicket({ organizationId: "tenant-id" }),
          sinkTicket,
        ]),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, retryDelay: 0 } },
      });
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />, { queryClient });
      await user.selectOptions(
        await screen.findByRole("combobox", {
          name: "Filter tickets by organization",
        }),
        "9e8b1bc8",
      );

      // A refresh no longer returns that organization's only ticket.
      server.use(
        ticketsHandler([buildTicket({ organizationId: "tenant-id" })]),
      );
      await queryClient.refetchQueries({
        queryKey: supportTicketQueryKeys.all(),
      });
      await waitFor(() =>
        expect(
          screen.queryByRole("link", { name: "Sink is lagging" }),
        ).not.toBeInTheDocument(),
      );

      const filter = screen.getByRole("combobox", {
        name: "Filter tickets by organization",
      });
      expect(filter).toHaveValue("9e8b1bc8-fa08-4870-a21f-22ac01cc3808");
      await user.selectOptions(filter, "All organizations");
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
    });

    it("only offers the organization filter with two or more organizations", async () => {
      server.use(
        ticketsHandler([
          buildTicket({ organizationId: "tenant-id" }),
          buildTicket({ id: "8", number: "4823", title: "From the Messenger" }),
        ]),
      );
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByRole("link", { name: "Replica keeps restarting" });
      expect(
        screen.queryByRole("combobox", {
          name: "Filter tickets by organization",
        }),
      ).not.toBeInTheDocument();
    });

    it("leaves out the organization and opener when no ticket has them", async () => {
      server.use(ticketsHandler([buildTicket()]));
      await renderComponent(<SupportPageWithIntercom />);

      const table = await screen.findByTestId("support-tickets-table");
      expect(
        within(table).queryByRole("columnheader", { name: /Organization/ }),
      ).not.toBeInTheDocument();
      expect(
        within(table).queryByRole("columnheader", { name: /Opened by/ }),
      ).not.toBeInTheDocument();
    });

    it("pages through the tickets", async () => {
      server.use(
        ticketsHandler(
          Array.from({ length: 12 }, (_, i) =>
            buildTicket({
              id: `${i + 1}`,
              number: `${5000 + i}`,
              title: `Ticket ${i + 1}`,
              updatedAt: `2026-10-${String(30 - i).padStart(2, "0")}T12:00:00Z`,
            }),
          ),
        ),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByRole("link", { name: "Ticket 1" }),
      ).toBeVisible();
      expect(screen.getByText(/Showing 1-10 of 12 tickets/)).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Ticket 11" }),
      ).not.toBeInTheDocument();

      await user.click(screen.getByRole("button", { name: "Next page" }));

      expect(screen.getByRole("link", { name: "Ticket 11" })).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Ticket 1" }),
      ).not.toBeInTheDocument();
    });

    it("stops loading after ten pages and points to Help Center", async () => {
      let requests = 0;
      server.use(
        http.get(TICKETS_URL, () => {
          requests += 1;
          return HttpResponse.json({
            data: [
              buildTicket({ id: `${requests}`, title: `Ticket ${requests}` }),
            ],
            nextCursor: `page-${requests + 1}`,
          });
        }),
      );
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByTestId("support-tickets-truncated"),
      ).toHaveTextContent("Older tickets are in Help Center");
      expect(requests).toBe(10);
    });

    it("doesn't say there are no tickets when ten pages come back empty", async () => {
      let requests = 0;
      server.use(
        http.get(TICKETS_URL, () => {
          requests += 1;
          return HttpResponse.json({
            data: [],
            nextCursor: `page-${requests + 1}`,
          });
        }),
      );
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByTestId("support-tickets-truncated"),
      ).toBeVisible();
      expect(requests).toBe(10);
      expect(screen.getByTestId("support-tickets-empty")).toHaveTextContent(
        "No tickets to show.",
      );
      expect(
        screen.queryByText(/You haven't opened any tickets yet/),
      ).not.toBeInTheDocument();
    });

    it.each([
      { secondPage: [buildTicket({ id: "7", title: "Sink is lagging" })] },
      { secondPage: [] },
    ])(
      "waits for later pages before saying no tickets match ($secondPage.length open on page 2)",
      async ({ secondPage }) => {
        let releaseSecondPage = () => {};
        const secondPageHeld = new Promise<void>((resolve) => {
          releaseSecondPage = resolve;
        });
        server.use(
          http.get(TICKETS_URL, async ({ request }) => {
            if (new URL(request.url).searchParams.get("cursor") !== "page-2") {
              return HttpResponse.json({
                data: [
                  buildTicket({
                    id: "9",
                    title: "RBAC for a dbt service account",
                    open: false,
                    state: "Resolved",
                    stateCategory: "resolved",
                  }),
                ],
                nextCursor: "page-2",
              });
            }
            await secondPageHeld;
            return HttpResponse.json({ data: secondPage, nextCursor: null });
          }),
        );
        await renderComponent(<SupportPageWithIntercom />);

        // Only a resolved ticket so far, hidden by the Open filter.
        expect(
          await screen.findByTestId("support-tickets-loading-more"),
        ).toBeVisible();
        expect(screen.getByRole("radio", { name: "Open" })).toBeChecked();
        expect(
          screen.queryByTestId("support-tickets-none-match"),
        ).not.toBeInTheDocument();

        releaseSecondPage();

        await waitFor(() =>
          expect(
            screen.queryByTestId("support-tickets-loading-more"),
          ).not.toBeInTheDocument(),
        );
        if (secondPage.length > 0) {
          expect(
            screen.getByRole("link", { name: "Sink is lagging" }),
          ).toBeVisible();
          expect(
            screen.queryByTestId("support-tickets-none-match"),
          ).not.toBeInTheDocument();
        } else {
          expect(
            screen.getByTestId("support-tickets-none-match"),
          ).toHaveTextContent("No open tickets.");
        }
      },
    );

    it("doesn't say no tickets match when a later page fails", async () => {
      server.use(
        http.get(TICKETS_URL, ({ request }) =>
          new URL(request.url).searchParams.get("cursor") === "page-2"
            ? HttpResponse.json({}, { status: 500 })
            : HttpResponse.json({
                data: [
                  buildTicket({
                    open: false,
                    state: "Resolved",
                    stateCategory: "resolved",
                  }),
                ],
                nextCursor: "page-2",
              }),
        ),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retryDelay: 0 } },
      });
      await renderComponent(<SupportPageWithIntercom />, { queryClient });

      expect(
        await screen.findByTestId("support-tickets-load-more-error"),
      ).toBeVisible();
      expect(screen.getByRole("radio", { name: "Open" })).toBeChecked();
      expect(
        screen.queryByTestId("support-tickets-none-match"),
      ).not.toBeInTheDocument();
    });

    it("keeps the loaded tickets when a later page fails", async () => {
      const requests: Record<string, number> = { first: 0, second: 0 };
      let isFailing = true;
      server.use(
        http.get(TICKETS_URL, ({ request }) => {
          const cursor = new URL(request.url).searchParams.get("cursor");
          if (cursor !== "page-2") {
            requests.first += 1;
            return HttpResponse.json({
              data: [buildTicket()],
              nextCursor: "page-2",
            });
          }
          requests.second += 1;
          return isFailing
            ? HttpResponse.json({}, { status: 500 })
            : HttpResponse.json({
                data: [buildTicket({ id: "7", title: "Sink is lagging" })],
                nextCursor: null,
              });
        }),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retryDelay: 0 } },
      });
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />, { queryClient });

      expect(
        await screen.findByTestId("support-tickets-load-more-error"),
      ).toHaveTextContent("Some of your tickets couldn't be loaded.");
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      // Only the failed page is retried, not the pages before it.
      expect(requests).toEqual({ first: 1, second: 3 });

      isFailing = false;
      await user.click(screen.getByRole("button", { name: "Try again" }));

      expect(
        await screen.findByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(
        screen.queryByTestId("support-tickets-load-more-error"),
      ).not.toBeInTheDocument();
      expect(requests).toEqual({ first: 1, second: 4 });
    });

    it("finishes refreshing stale tickets before loading more pages", async () => {
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retryDelay: 0 } },
      });
      // Hovering Help prefetched the first page over a minute ago.
      queryClient.setQueryData(
        supportTicketsQueryOptions({
          userId: dummyValidUser.id,
          organizationId: dummyValidUser.tenantId,
        }).queryKey,
        {
          pages: [{ data: [buildTicket()], nextCursor: "page-2" }],
          pageParams: [undefined],
        },
        { updatedAt: Date.now() - 120_000 },
      );
      server.use(
        http.get(TICKETS_URL, ({ request }) =>
          HttpResponse.json(
            new URL(request.url).searchParams.get("cursor") === "page-2"
              ? {
                  data: [buildTicket({ id: "3", title: "Older ticket" })],
                  nextCursor: null,
                }
              : {
                  data: [
                    buildTicket({ id: "7", title: "Sink is lagging" }),
                    buildTicket(),
                  ],
                  nextCursor: "page-2",
                },
          ),
        ),
      );
      await renderComponent(<SupportPageWithIntercom />, { queryClient });

      // The refresh isn't cancelled, so the ticket opened since shows up.
      expect(
        await screen.findByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(
        await screen.findByRole("link", { name: "Older ticket" }),
      ).toBeVisible();
    });

    it("keeps looking when a page has no visible tickets", async () => {
      server.use(
        http.get(TICKETS_URL, ({ request }) => {
          const cursor = new URL(request.url).searchParams.get("cursor");
          return HttpResponse.json(
            cursor === "page-2"
              ? { data: [buildTicket()], nextCursor: null }
              : { data: [], nextCursor: "page-2" },
          );
        }),
      );
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(
        screen.queryByTestId("support-tickets-empty"),
      ).not.toBeInTheDocument();
    });

    it("keeps the tickets when a refetch fails", async () => {
      server.use(ticketsHandler([buildTicket()]));
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, retryDelay: 0 } },
      });
      await renderComponent(<SupportPageWithIntercom />, { queryClient });
      expect(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();

      server.use(
        http.get(TICKETS_URL, () => HttpResponse.json({}, { status: 500 })),
      );
      await queryClient.refetchQueries({
        queryKey: supportTicketQueryKeys.all(),
      });

      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(screen.queryByRole("link", { name: "View tickets" })).toBeNull();
    });

    it("opens a ticket from anywhere in its row", async () => {
      server.use(ticketsHandler([buildTicket()]));
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await user.click(await screen.findByText("#4821"));

      expect(await screen.findByRole("dialog")).toBeVisible();
    });

    it("offers a retry when the tickets fail to load", async () => {
      let isFailing = true;
      server.use(
        http.get(TICKETS_URL, () =>
          isFailing
            ? HttpResponse.json({}, { status: 500 })
            : HttpResponse.json({ data: [buildTicket()], nextCursor: null }),
        ),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, retryDelay: 0 } },
      });
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />, { queryClient });

      expect(
        await screen.findByText(/Your tickets couldn't be loaded/),
      ).toBeVisible();
      // The fallback for missing endpoints isn't shown for a failure.
      expect(screen.queryByText(/See the tickets you opened/)).toBeNull();
      isFailing = false;
      await user.click(screen.getByRole("button", { name: "Try again" }));

      expect(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
    });

    it("filters tickets by status", async () => {
      server.use(
        ticketsHandler([
          buildTicket(),
          buildTicket({
            id: "43",
            title: "RBAC for a dbt service account",
            state: "Resolved",
            stateCategory: "resolved",
            open: false,
          }),
        ]),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByRole("link", { name: "Replica keeps restarting" });
      await user.click(screen.getByRole("radio", { name: "Resolved" }));

      expect(
        screen.getByRole("link", { name: "RBAC for a dbt service account" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Replica keeps restarting" }),
      ).not.toBeInTheDocument();

      await user.click(screen.getByRole("radio", { name: "All tickets" }));

      expect(
        screen.getByRole("link", { name: "RBAC for a dbt service account" }),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "Replica keeps restarting" }),
      ).toBeVisible();
    });

    it("shows when a ticket was updated, with the full date on hover", async () => {
      server.use(ticketsHandler([buildTicket()]));
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByRole("link", { name: "Replica keeps restarting" });
      const table = screen.getByTestId("support-tickets-table");
      expect(
        within(table).getByText(
          (_, element) =>
            element?.getAttribute("datetime") === "2026-10-21T12:00:00Z",
        ),
      ).toBeVisible();
    });

    it("doesn't retry requests that can't succeed", async () => {
      let requests = 0;
      server.use(
        http.get(TICKETS_URL, () => {
          requests += 1;
          return HttpResponse.json({}, { status: 403 });
        }),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, retryDelay: 0 } },
      });
      await renderComponent(<SupportPageWithIntercom />, { queryClient });

      expect(
        await screen.findByText(/Your tickets couldn't be loaded/),
      ).toBeVisible();
      expect(requests).toBe(1);
    });

    it("shows the ticket's title and state while its replies load", async () => {
      server.use(
        ticketsHandler([buildTicket()]),
        http.get(
          `${TICKETS_URL}/:ticketId`,
          () => new Promise<never>(() => {}),
        ),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await user.click(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      );

      const dialog = await screen.findByRole("dialog");
      expect(dialog).toHaveTextContent("Replica keeps restarting");
      expect(within(dialog).getByText("Waiting on you")).toBeVisible();
      expect(screen.queryByTestId("support-ticket")).not.toBeInTheDocument();
    });

    it("shows an empty state", async () => {
      server.use(ticketsHandler([]));
      await renderComponent(<SupportPageWithIntercom />);

      expect(await screen.findByTestId("support-tickets-empty")).toBeVisible();
    });

    it("shows a ticket's conversation", async () => {
      server.use(
        ticketsHandler([buildTicket()]),
        ticketDetailHandler({
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
      );
      const user = userEvent.setup();
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <CurrentPath />
        </>,
      );

      await user.click(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      );

      const ticket = await screen.findByTestId("support-ticket");
      expect(ticket).toHaveTextContent("It restarts every few minutes.");
      expect(ticket).toHaveTextContent("Katrina");
      expect(ticket).toHaveTextContent("Thanks, looking now.");
      expect(
        screen.getByRole("link", { name: "Reply in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
      expect(screen.getByTestId("current-path")).toHaveTextContent(
        "/support/tickets/42",
      );

      // The footer's Close; the header has an icon button with the same name.
      const closeButtons = screen.getAllByRole("button", { name: "Close" });
      await user.click(closeButtons[closeButtons.length - 1]);
      await waitFor(() =>
        expect(screen.getByTestId("current-path")).toHaveTextContent(
          /^\/support$/,
        ),
      );
    });

    it("shows who opened a teammate's ticket", async () => {
      server.use(
        ticketsHandler([buildTicket({ openedBy: "teammate@example.com" })]),
        ticketDetailHandler({
          ticket: buildTicket({ openedBy: "teammate@example.com" }),
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await user.click(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      );

      const ticket = await screen.findByTestId("support-ticket");
      expect(ticket).toHaveTextContent("teammate@example.com");
      expect(ticket).not.toHaveTextContent("You");
    });

    it("doesn't reopen a closed ticket on Back", async () => {
      server.use(ticketsHandler([buildTicket()]), ticketDetailHandler());
      const user = userEvent.setup();
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <BackButton />
          <CurrentPath />
        </>,
        { initialRouterEntries: ["/support"] },
      );

      await user.click(
        await screen.findByRole("link", { name: "Replica keeps restarting" }),
      );
      await screen.findByTestId("support-ticket");
      const closeButtons = screen.getAllByRole("button", { name: "Close" });
      await user.click(closeButtons[closeButtons.length - 1]);
      await waitFor(() =>
        expect(screen.queryByTestId("support-ticket")).not.toBeInTheDocument(),
      );

      await user.click(screen.getByRole("button", { name: "Back" }));

      expect(screen.getByTestId("current-path")).toHaveTextContent(
        /^\/support$/,
      );
      expect(screen.queryByTestId("support-ticket")).not.toBeInTheDocument();
    });

    it("opens a ticket from its URL", async () => {
      server.use(ticketsHandler([buildTicket()]), ticketDetailHandler());
      await renderComponent(<SupportPageWithIntercom />, {
        initialRouterEntries: ["/support/tickets/42"],
      });

      expect(await screen.findByTestId("support-ticket")).toHaveTextContent(
        "It restarts every few minutes.",
      );
    });

    it("sends unknown support paths to the support page", async () => {
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <CurrentPath />
        </>,
        { initialRouterEntries: ["/support/nope"] },
      );

      await waitFor(() =>
        expect(screen.getByTestId("current-path")).toHaveTextContent(
          /^\/support$/,
        ),
      );
    });
  });

  describe("the ticket form", () => {
    beforeEach(() => {
      mockFlags["support-ticket-form-cns-201"] = true;
    });

    afterEach(() => {
      delete mockFlags["support-ticket-form-cns-201"];
    });

    const createHandler = (
      onRequest: (body: unknown) => void,
      status = 201,
      pagesOnCall = false,
    ) =>
      http.post(TICKETS_URL, async ({ request }) => {
        onRequest(await request.json());
        return status === 201
          ? HttpResponse.json(
              { ...buildTicket({ title: "Sink is lagging" }), pagesOnCall },
              { status },
            )
          : HttpResponse.json({}, { status });
      });

    it("keeps the Messenger and email links while the form is off", async () => {
      delete mockFlags["support-ticket-form-cns-201"];
      server.use(ticketsHandler([]));
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      expect(
        screen.getByRole("link", { name: "Open a ticket" }),
      ).toHaveAttribute("href", SUPPORT_MAILTO);
      expect(
        screen.getByRole("link", { name: "Report a production outage" }),
      ).toHaveAttribute("href", OUTAGE_MAILTO);
    });

    it("validates the form and opens a ticket", async () => {
      let createdWith: unknown;
      server.use(
        ticketsHandler([]),
        createHandler((body) => {
          createdWith = body;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      expect(showNewMessage).not.toHaveBeenCalled();

      await user.click(
        await screen.findByRole("button", { name: "Open ticket" }),
      );
      const title = screen.getByRole("textbox", { name: /Subject/ });
      expect(await screen.findByText("A subject is required.")).toBeVisible();
      expect(screen.getByText("Describe what happened.")).toBeVisible();
      expect(title).toHaveAttribute("aria-invalid", "true");
      expect(createdWith).toBeUndefined();

      // The default form leaves urgent to the outage form.
      expect(
        screen.queryByRole("radio", { name: /^Urgent/ }),
      ).not.toBeInTheDocument();
      await user.type(title, "Sink is lagging");
      await user.click(screen.getByRole("radio", { name: /^High/ }));
      await user.click(screen.getByRole("radio", { name: /^Staging/ }));
      const details = screen.getByRole("textbox", { name: /What happened/ });
      expect(details).toHaveValue("");
      expect(details).toHaveAttribute("placeholder", TICKET_TEMPLATE);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      expect(screen.getByText("Describe what happened.")).toBeVisible();
      expect(createdWith).toBeUndefined();
      await user.type(details, "Kafka sink is 10 minutes behind.");
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(createdWith).toMatchObject({
        title: "Sink is lagging",
        description: "Kafka sink is 10 minutes behind.",
        severity: "high",
        environmentType: "staging",
        organizationId: "org_id",
        region: defaultRegionId,
      });
      // The API couldn't list other organizations, so there's no choice.
      expect(
        screen.queryByRole("combobox", { name: "Organization and region" }),
      ).not.toBeInTheDocument();
      expect(sent).toHaveTextContent("Ticket #4821 is open");
      expect(sent).toHaveTextContent(defaultRegionId);
      expect(
        within(sent).getByRole("link", { name: "View in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    });

    const attachHandler = (onRequest: (body: unknown) => void, status = 200) =>
      http.post(
        `${TICKETS_URL}/:ticketId/attachments`,
        async ({ request, params }) => {
          const body = (await request.json()) as Record<string, unknown>;
          onRequest({ ticketId: params.ticketId, ...body });
          return status === 200
            ? HttpResponse.json({ attached: 2 })
            : HttpResponse.json({}, { status });
        },
      );

    it("adds attached files to the ticket once it's open", async () => {
      let attachedWith: Record<string, unknown> | undefined;
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        attachHandler((body) => {
          attachedWith = body as Record<string, unknown>;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      const attach = await screen.findByRole("button", {
        name: "Attach files",
      });
      // The limits are in the button's tooltip.
      await user.hover(attach);
      expect(await screen.findByText(ATTACHMENT_LIMITS)).toBeInTheDocument();

      await user.upload(screen.getByTestId("support-ticket-file-input"), [
        new File(["hi"], "error.txt", { type: "text/plain" }),
        new File(["png"], "screen.png", { type: "image/png" }),
        new File(["extra"], "extra.txt", { type: "text/plain" }),
      ]);
      await user.click(
        screen.getByRole("button", { name: "Remove extra.txt" }),
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      await waitFor(() =>
        expect(
          within(sent).getByTestId("support-ticket-attachments-status"),
        ).toHaveTextContent("2 files added to your ticket."),
      );
      expect(attachedWith).toEqual({
        ticketId: "42",
        files: [
          { name: "error.txt", data: window.btoa("hi") },
          { name: "screen.png", data: window.btoa("png") },
        ],
      });
      // More files can still go by email or the Help Center.
      const note = within(sent).getByTestId("support-ticket-files-note");
      expect(note).toHaveTextContent("up to 20 MB in total");
      expect(
        within(note).getByRole("link", { name: "Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    });

    it("keeps keyboard focus in the file picker when files are removed", async () => {
      server.use(ticketsHandler([]));
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      const input = await screen.findByTestId("support-ticket-file-input");
      // The Attach files button opens it; the hidden input isn't a tab stop.
      expect(input).toHaveAttribute("tabindex", "-1");
      await user.upload(input, [
        new File(["hi"], "error.txt", { type: "text/plain" }),
        new File(["png"], "screen.png", { type: "image/png" }),
      ]);

      await user.click(
        screen.getByRole("button", { name: "Remove error.txt" }),
      );
      await waitFor(() =>
        expect(
          screen.getByRole("button", { name: "Remove screen.png" }),
        ).toHaveFocus(),
      );
      await user.click(
        screen.getByRole("button", { name: "Remove screen.png" }),
      );
      await waitFor(() =>
        expect(
          screen.getByRole("button", { name: "Attach files" }),
        ).toHaveFocus(),
      );
    });

    it("doesn't open a ticket with files that can't be attached", async () => {
      let createdWith: unknown;
      server.use(
        ticketsHandler([]),
        createHandler((body) => {
          createdWith = body;
        }),
      );
      // e.g. dropped in, past the file picker's filter.
      const user = userEvent.setup({ applyAccept: false });
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        new File(["zip"], "debug.zip"),
      );
      expect(
        within(screen.getByTestId("support-ticket-attachments")).getByRole(
          "alert",
        ),
      ).toHaveTextContent("debug.zip can't be attached");
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      expect(createdWith).toBeUndefined();

      await user.click(
        screen.getByRole("button", { name: "Remove debug.zip" }),
      );
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      await screen.findByTestId("support-ticket-sent");
      expect(createdWith).toBeDefined();
    });

    it("keeps the ticket open when its files can't be added", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        attachHandler(() => {}, 502),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        new File(["hi"], "error.txt", { type: "text/plain" }),
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(sent).toHaveTextContent("Ticket #4821 is open");
      expect(
        await within(sent).findByTestId("support-ticket-attachments-status"),
      ).toHaveTextContent(
        "Your ticket is open, but the 1 file couldn't be added.",
      );
      // Says how to send it instead.
      expect(
        within(sent).getByTestId("support-ticket-files-note"),
      ).toBeVisible();
    });

    it("adds the files even if the drawer closes before the ticket opens", async () => {
      let openTicket = () => {};
      const ticketOpened = new Promise<void>((resolve) => {
        openTicket = resolve;
      });
      const attached: unknown[] = [];
      server.use(
        ticketsHandler([]),
        http.post(TICKETS_URL, async () => {
          await ticketOpened;
          return HttpResponse.json(
            {
              ...buildTicket({ title: "Sink is lagging" }),
              pagesOnCall: false,
            },
            { status: 201 },
          );
        }),
        attachHandler((body) => {
          attached.push(body);
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        new File(["hi"], "error.txt", { type: "text/plain" }),
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      await user.click(
        screen.getByRole("button", { name: "Back to help and support" }),
      );
      await waitFor(() =>
        expect(
          screen.queryByRole("button", { name: "Open ticket" }),
        ).not.toBeInTheDocument(),
      );

      openTicket();
      await waitFor(() =>
        expect(attached).toEqual([
          {
            ticketId: "42",
            files: [{ name: "error.txt", data: window.btoa("hi") }],
          },
        ]),
      );
    });

    it("says to check the ticket when the files may have been added", async () => {
      let attachRequests = 0;
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        attachHandler(() => {
          attachRequests++;
        }, 504),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        [
          new File(["hi"], "error.txt", { type: "text/plain" }),
          new File(["png"], "screen.png", { type: "image/png" }),
        ],
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(
        await within(sent).findByTestId("support-ticket-attachments-status"),
      ).toHaveTextContent(
        "Your ticket is open, but we couldn't confirm the 2 files were added. Check the ticket in Help Center before adding them again.",
      );
      // Sending them again could add them twice.
      expect(attachRequests).toBe(1);
    });

    it("treats a network drop while adding files as unconfirmed", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        http.post(`${TICKETS_URL}/:ticketId/attachments`, () =>
          HttpResponse.error(),
        ),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        new File(["hi"], "error.txt", { type: "text/plain" }),
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(
        await within(sent).findByTestId("support-ticket-attachments-status"),
      ).toHaveTextContent(
        "Your ticket is open, but we couldn't confirm the 1 file was added. Check the ticket in Help Center before adding them again.",
      );
    });

    it("keeps the files as they were while the ticket opens", async () => {
      let openTicket = () => {};
      const ticketOpened = new Promise<void>((resolve) => {
        openTicket = resolve;
      });
      const attached: unknown[] = [];
      server.use(
        ticketsHandler([]),
        http.post(TICKETS_URL, async () => {
          await ticketOpened;
          return HttpResponse.json(
            {
              ...buildTicket({ title: "Sink is lagging" }),
              pagesOnCall: false,
            },
            { status: 201 },
          );
        }),
        attachHandler((body) => {
          attached.push(body);
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        [
          new File(["hi"], "error.txt", { type: "text/plain" }),
          new File(["png"], "screen.png", { type: "image/png" }),
        ],
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      // The ticket gets the files picked when it was sent.
      expect(
        screen.getByRole("button", { name: "Remove error.txt" }),
      ).toBeDisabled();
      expect(
        screen.getByRole("button", { name: "Attach files" }),
      ).toBeDisabled();
      expect(screen.getByTestId("support-ticket-file-input")).toBeDisabled();

      openTicket();
      await screen.findByTestId("support-ticket-sent");
      await waitFor(() =>
        expect(attached).toEqual([
          {
            ticketId: "42",
            files: [
              { name: "error.txt", data: window.btoa("hi") },
              { name: "screen.png", data: window.btoa("png") },
            ],
          },
        ]),
      );
    });

    it("says when files fail after the confirmation closes", async () => {
      let failUpload = () => {};
      const uploadFailed = new Promise<void>((resolve) => {
        failUpload = resolve;
      });
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        http.post(`${TICKETS_URL}/:ticketId/attachments`, async () => {
          await uploadFailed;
          return HttpResponse.json({}, { status: 502 });
        }),
        ticketDetailHandler(),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.upload(
        await screen.findByTestId("support-ticket-file-input"),
        new File(["hi"], "error.txt", { type: "text/plain" }),
      );
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      const sent = await screen.findByTestId("support-ticket-sent");
      expect(
        await within(sent).findByTestId("support-ticket-attachments-status"),
      ).toHaveTextContent("Adding 1 file\u2026");
      await user.click(within(sent).getByRole("button", { name: "Done" }));
      await waitFor(() =>
        expect(
          screen.queryByTestId("support-ticket-sent"),
        ).not.toBeInTheDocument(),
      );

      failUpload();
      const toast = await screen.findByText(
        "Ticket #4821 is open, but the 1 file couldn't be added. Reply to the confirmation email or open the ticket in Help Center to share them.",
      );
      // It stays until it's closed.
      await user.click(
        within(toast.closest("[role=status]") ?? document.body).getByRole(
          "button",
          { name: "Close" },
        ),
      );
      await waitFor(() => expect(toast).not.toBeInTheDocument());

      // The ticket says so too.
      await user.click(
        await screen.findByRole("link", { name: "Sink is lagging" }),
      );
      expect(
        await screen.findByTestId("support-ticket-attachments-status"),
      ).toHaveTextContent(
        "Your ticket is open, but the 1 file couldn't be added. Reply to the confirmation email or open the ticket in Help Center to share them.",
      );
    });

    it("only retries adding files when the server turned them away unread", () => {
      const status = (code: number) => new OpenApiFetchError(code, {});
      expect(retryWhenUploadsAreBusy(0, status(503))).toBe(true);
      expect(retryWhenUploadsAreBusy(1, status(503))).toBe(true);
      expect(retryWhenUploadsAreBusy(2, status(503))).toBe(false);
      for (const code of [400, 404, 413, 500, 502, 504]) {
        expect(retryWhenUploadsAreBusy(0, status(code))).toBe(false);
      }
      // A network error may have happened after the files arrived.
      expect(retryWhenUploadsAreBusy(0, new TypeError("Failed to fetch"))).toBe(
        false,
      );
    });

    it("treats only request-level ambiguity as unconfirmed", () => {
      expect(isAttachUnconfirmedError(new OpenApiFetchError(504, {}))).toBe(
        true,
      );
      expect(isAttachUnconfirmedError(new TypeError("Failed to fetch"))).toBe(
        true,
      );
      expect(isAttachUnconfirmedError(new OpenApiFetchError(502, {}))).toBe(
        false,
      );
      // FileReader failures happen before anything is sent.
      expect(
        isAttachUnconfirmedError(new DOMException("NotReadableError")),
      ).toBe(false);
    });

    it("opens a ticket without files as before", async () => {
      let attachRequests = 0;
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
        attachHandler(() => {
          attachRequests++;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(
        within(sent).queryByTestId("support-ticket-attachments-status"),
      ).not.toBeInTheDocument();
      expect(attachRequests).toBe(0);
    });

    it("keeps a new ticket in the list before the search catches up", async () => {
      let listRequests = 0;
      server.use(
        // The search doesn't include the new ticket yet.
        http.get(TICKETS_URL, () => {
          listRequests++;
          return HttpResponse.json({ data: [], nextCursor: null });
        }),
        createHandler(() => {}),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      await user.click(await screen.findByRole("button", { name: "Done" }));

      expect(
        await screen.findByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();

      act(() => {
        focusManager.setFocused(false);
        focusManager.setFocused(true);
      });
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(
        screen.getByRole("link", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(listRequests).toBe(1);
    });

    it("starts an outage report from the confirmation", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
      );
      const user = userEvent.setup();
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <CurrentPath />
        </>,
      );

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));
      const sent = await screen.findByTestId("support-ticket-sent");

      await user.click(
        within(sent).getByRole("button", {
          name: "Report a production outage",
        }),
      );

      expect(
        await screen.findByTestId("support-ticket-urgent-notice"),
      ).toBeVisible();
      expect(screen.getByRole("textbox", { name: /Subject/ })).toHaveValue("");
      expect(screen.getByTestId("current-path")).toHaveTextContent(
        /^\/support\/new$/,
      );
    });

    it("offers each organization's active regions", async () => {
      let createdWith: Record<string, unknown> | undefined;
      server.use(
        ticketsHandler([]),
        http.get(ORGANIZATIONS_URL, () =>
          HttpResponse.json({
            data: [
              {
                id: "sandbox-org",
                name: "Sandbox",
                regions: [
                  { id: "aws/eu-west-1", environmentVersion: "v26.47.0" },
                  { id: "aws/us-west-2", environmentVersion: null },
                ],
              },
              {
                id: "org_id",
                name: "Test Organization",
                regions: [{ id: defaultRegionId, environmentVersion: null }],
              },
            ],
          }),
        ),
        createHandler((body) => {
          createdWith = body as Record<string, unknown>;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      const choice = await screen.findByRole("combobox", {
        name: "Organization and region",
      });
      expect(
        within(choice)
          .getAllByRole("option")
          .map((option) => option.textContent),
      ).toEqual([
        "Sandbox \u00b7 aws/eu-west-1",
        "Sandbox \u00b7 aws/us-west-2",
        `Test Organization \u00b7 ${defaultRegionId}`,
      ]);
      // It starts on the signed-in organization and region.
      expect(choice).toHaveValue(`org_id ${defaultRegionId}`);

      await user.selectOptions(choice, "Sandbox \u00b7 aws/eu-west-1");
      const context = screen.getByTestId("support-ticket-context");
      expect(context).toHaveTextContent("aws/eu-west-1");
      expect(context).toHaveTextContent("v26.47.0");
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      await screen.findByTestId("support-ticket-sent");
      expect(createdWith).toMatchObject({
        organizationId: "sandbox-org",
        region: "aws/eu-west-1",
        environmentVersion: "v26.47.0",
      });
    });

    it("opens a ticket for another of the user's organizations", async () => {
      let createdWith: Record<string, unknown> | undefined;
      server.use(
        ticketsHandler([]),
        http.get(ORGANIZATIONS_URL, () =>
          HttpResponse.json({
            data: [
              { id: "sandbox-org", name: "Sandbox" },
              { id: "org_id", name: "Test Organization" },
            ],
          }),
        ),
        createHandler((body) => {
          createdWith = body as Record<string, unknown>;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      // An older API lists the organizations without their regions.
      const organization = await screen.findByRole("combobox", {
        name: "Organization and region",
      });
      // It starts on the signed-in organization and the current region.
      expect(organization).toHaveValue(`org_id ${defaultRegionId}`);
      const context = screen.getByTestId("support-ticket-context");
      expect(context).toHaveTextContent(defaultRegionId);

      await user.selectOptions(organization, "Sandbox");
      // The console only knows the signed-in organization's region.
      expect(context).toHaveTextContent("Sandbox");
      expect(context).not.toHaveTextContent(defaultRegionId);
      await fillTicketForm(user);
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(sent).toHaveTextContent("Sandbox");
      expect(createdWith).toMatchObject({ organizationId: "sandbox-org" });
      expect(createdWith).not.toHaveProperty("region");
      expect(createdWith).not.toHaveProperty("environmentVersion");
    });

    it("keeps the form open with an error when opening fails", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}, 502),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await fillTicketForm(user, {
        title: "Sink is lagging",
        description: "Kafka sink is 10 minutes behind.",
      });
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      expect(
        await screen.findByText(/Your ticket couldn't be opened/),
      ).toBeVisible();
      expect(screen.getByRole("textbox", { name: /Subject/ })).toHaveValue(
        "Sink is lagging",
      );
    });

    it("asks to email support when the support contact needs linking", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}, 409),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await fillTicketForm(user, {
        title: "Sink is lagging",
        description: "Kafka sink is 10 minutes behind.",
      });
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      expect(
        await screen.findByText(/support account needs to be linked/),
      ).toBeVisible();
    });

    it("opens the form on Urgent from the outage button", async () => {
      setIntercom("booted");
      server.use(ticketsHandler([]));
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(
        screen.getByRole("button", { name: "Report a production outage" }),
      );

      expect(
        await screen.findByTestId("support-ticket-urgent-notice"),
      ).toHaveTextContent("Plans with an SLA will page on-call right away.");
      // Outages are urgent and in production, so neither is a choice.
      expect(screen.queryByRole("radio")).not.toBeInTheDocument();
      expect(
        screen.getByTestId("support-ticket-environment"),
      ).toHaveTextContent("Production");
      expect(
        screen.getByRole("textbox", { name: /What happened/ }),
      ).toHaveAttribute("placeholder", OUTAGE_TEMPLATE);
      expect(
        screen.getByRole("link", { name: "How to run mz-debug ↗" }),
      ).toHaveAttribute("href", MZ_DEBUG_SELF_MANAGED_URL);
      expect(showNewMessage).not.toHaveBeenCalled();
    });

    it("sends production outages to the outage form", async () => {
      server.use(ticketsHandler([]));
      const user = userEvent.setup();
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <CurrentPath />
        </>,
      );

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.click(
        within(
          await screen.findByTestId("support-ticket-outage-callout"),
        ).getByRole("button", { name: "Report a production outage" }),
      );

      expect(
        await screen.findByTestId("support-ticket-urgent-notice"),
      ).toBeVisible();
      expect(
        screen.getByRole("button", { name: "Report outage" }),
      ).toBeVisible();
    });

    it("doesn't reopen a closed form on Back", async () => {
      server.use(ticketsHandler([]));
      const user = userEvent.setup();
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <BackButton />
          <CurrentPath />
        </>,
        { initialRouterEntries: ["/support"] },
      );

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      await user.click(await screen.findByRole("button", { name: "Cancel" }));
      await waitFor(() =>
        expect(
          screen.queryByRole("textbox", { name: /Subject/ }),
        ).not.toBeInTheDocument(),
      );

      await user.click(screen.getByRole("button", { name: "Back" }));

      expect(screen.getByTestId("current-path")).toHaveTextContent(
        /^\/support$/,
      );
      expect(
        screen.queryByRole("textbox", { name: /Subject/ }),
      ).not.toBeInTheDocument();
    });

    it("opens the form from its URL", async () => {
      server.use(ticketsHandler([]));
      await renderComponent(<SupportPageWithIntercom />, {
        initialRouterEntries: ["/support/new"],
      });

      expect(
        await screen.findByRole("textbox", { name: /Subject/ }),
      ).toBeVisible();
    });

    it("sends the form's URL to the support page when the form is unavailable", async () => {
      await renderComponent(
        <>
          <SupportPageWithIntercom />
          <CurrentPath />
        </>,
        { initialRouterEntries: ["/support/new"] },
      );

      await waitFor(() =>
        expect(screen.getByTestId("current-path")).toHaveTextContent(
          /^\/support$/,
        ),
      );
      expect(
        screen.queryByRole("textbox", { name: /Subject/ }),
      ).not.toBeInTheDocument();
    });

    it("opens urgent tickets and says when on-call was paged", async () => {
      let createdWith: unknown;
      server.use(
        ticketsHandler([]),
        createHandler(
          (body) => {
            createdWith = body;
          },
          201,
          true,
        ),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />, {
        initialRouterEntries: ["/support/new?severity=urgent"],
      });

      await fillTicketForm(user, { title: "Production is down" });
      await user.click(screen.getByRole("button", { name: "Report outage" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(createdWith).toMatchObject({
        title: "Production is down",
        severity: "urgent",
        environmentType: "production",
      });
      expect(sent).toHaveTextContent("On-call has been paged.");
      expect(
        within(sent).queryByRole("button", {
          name: "Report a production outage",
        }),
      ).not.toBeInTheDocument();
    });

    it("doesn't claim on-call was paged when it wasn't", async () => {
      server.use(
        ticketsHandler([]),
        createHandler(() => {}),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />, {
        initialRouterEntries: ["/support/new?severity=urgent"],
      });

      await fillTicketForm(user, { title: "Production is down" });
      await user.click(screen.getByRole("button", { name: "Report outage" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(sent).not.toHaveTextContent("On-call has been paged.");
      expect(sent).toHaveTextContent("Urgent response target");
    });
  });
});
