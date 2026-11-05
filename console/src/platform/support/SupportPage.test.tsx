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
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { SupportTicket, SupportTicketDetail } from "~/api/cloudGlobalApi";
import { STATUSPAGE_URL } from "~/api/incident-io";
import { Incident, StatusSummary } from "~/api/incident-io/types";
import server from "~/api/mocks/server";
import {
  HELP_CENTER_TICKETS_URL,
  HELP_CENTER_URL,
  STATUS_PAGE_URL,
  SUPPORT_CHAT_URL,
  SUPPORT_PLANS_URL,
} from "~/externalUrls";
import { resetMessengerStateForTests, useIntercom } from "~/hooks/useIntercom";
import { incidentIOQueryKeys } from "~/queries/incident-io";
import { defaultRegionId, renderComponent } from "~/test/utils";

import { supportTicketQueryKeys } from "./queries";
import SupportPage, { OUTAGE_MAILTO, SUPPORT_MAILTO } from "./SupportPage";

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
  await user.type(
    screen.getByRole("textbox", { name: /What happened/ }),
    description,
  );
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
    );
  });

  afterEach(() => {
    setIntercom("missing");
    resetMessengerStateForTests();
    vi.clearAllMocks();
  });

  it("shows the organization and its ID", async () => {
    await renderComponent(<SupportPageWithIntercom />);

    expect(
      await screen.findByRole("heading", { name: "Help and support" }),
    ).toBeVisible();
    expect(await screen.findByText("Test Organization")).toBeVisible();
    expect(screen.getByTestId("support-org-id")).toHaveTextContent(
      "Org ID org_id",
    );
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      expect(screen.getByText("#4821")).toBeVisible();
      expect(screen.getByText("Waiting on you")).toBeVisible();
      expect(
        screen.getByRole("button", { name: "RBAC for a dbt service account" }),
      ).toBeVisible();
      expect(
        screen.getByRole("link", { name: "View in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
      expect(screen.queryByRole("link", { name: "View tickets" })).toBeNull();
    });

    it("loads more tickets", async () => {
      server.use(
        http.get(TICKETS_URL, ({ request }) => {
          const cursor = new URL(request.url).searchParams.get("cursor");
          return HttpResponse.json(
            cursor === "page-2"
              ? {
                  data: [buildTicket({ id: "7", title: "Sink is lagging" })],
                  nextCursor: null,
                }
              : { data: [buildTicket()], nextCursor: "page-2" },
          );
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      expect(
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
      ).toBeVisible();
      await user.click(screen.getByRole("button", { name: "Load more" }));

      expect(
        await screen.findByRole("button", { name: "Sink is lagging" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("button", { name: "Load more" }),
      ).not.toBeInTheDocument();
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
      ).toBeVisible();

      server.use(
        http.get(TICKETS_URL, () => HttpResponse.json({}, { status: 500 })),
      );
      await queryClient.refetchQueries({
        queryKey: supportTicketQueryKeys.all(),
      });

      expect(
        screen.getByRole("button", { name: "Replica keeps restarting" }),
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
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

      await screen.findByRole("button", { name: "Replica keeps restarting" });
      await user.click(
        screen.getByRole("combobox", { name: "Filter tickets by status" }),
      );
      // Options carry their label as a title.
      await user.click(screen.getByTitle("Resolved"));

      expect(
        screen.getByRole("button", { name: "RBAC for a dbt service account" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("button", { name: "Replica keeps restarting" }),
      ).not.toBeInTheDocument();
    });

    it("shows when a ticket was updated, with the full date on hover", async () => {
      server.use(ticketsHandler([buildTicket()]));
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByRole("button", { name: "Replica keeps restarting" });
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
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
        await screen.findByRole("button", { name: "Replica keeps restarting" }),
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

      await user.type(title, "Sink is lagging");
      await user.click(screen.getByRole("radio", { name: /^High/ }));
      await user.click(screen.getByRole("radio", { name: /^Staging/ }));
      await user.type(
        screen.getByRole("textbox", { name: /What happened/ }),
        "Kafka sink is 10 minutes behind.",
      );
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(createdWith).toMatchObject({
        title: "Sink is lagging",
        description: "Kafka sink is 10 minutes behind.",
        severity: "high",
        environmentType: "staging",
      });
      expect(sent).toHaveTextContent("Ticket #4821 is open");
      expect(sent).toHaveTextContent(defaultRegionId);
      expect(
        within(sent).getByRole("link", { name: "View in Help Center ↗" }),
      ).toHaveAttribute("href", HELP_CENTER_TICKETS_URL);
    });

    it("shows a new ticket in the list before the search catches up", async () => {
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
        await screen.findByRole("button", { name: "Sink is lagging" }),
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
        await screen.findByRole("radio", { name: /^Urgent/ }),
      ).toBeChecked();
      expect(screen.getByRole("textbox", { name: /Subject/ })).toHaveValue("");
      expect(screen.getByTestId("current-path")).toHaveTextContent(
        /^\/support\/new$/,
      );
    });

    it("leaves out environment details when unchecked", async () => {
      let createdWith: Record<string, unknown> | undefined;
      server.use(
        ticketsHandler([]),
        createHandler((body) => {
          createdWith = body as Record<string, unknown>;
        }),
      );
      const user = userEvent.setup();
      await renderComponent(<SupportPageWithIntercom />);

      await screen.findByTestId("support-tickets-empty");
      await user.click(screen.getByRole("button", { name: "Open a ticket" }));
      const context = await screen.findByTestId("support-ticket-context");
      expect(context).toHaveTextContent(defaultRegionId);
      await user.click(
        screen.getByRole("checkbox", { name: /Include environment details/ }),
      );
      // The org ID is always sent, so it stays listed.
      expect(context).toHaveTextContent("Organization");
      expect(context).not.toHaveTextContent(defaultRegionId);
      await fillTicketForm(user, {
        title: "Question",
        description: "How do I size a cluster?",
      });
      await user.click(screen.getByRole("button", { name: "Open ticket" }));

      const sent = await screen.findByTestId("support-ticket-sent");
      expect(createdWith).not.toHaveProperty("region");
      expect(createdWith).not.toHaveProperty("environmentVersion");
      // The confirmation only lists what was sent.
      expect(sent).not.toHaveTextContent(defaultRegionId);
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
        await screen.findByRole("radio", { name: /^Urgent/ }),
      ).toBeChecked();
      expect(
        screen.getByTestId("support-ticket-urgent-notice"),
      ).toHaveTextContent(/On plans with an SLA, they page on-call/);
      expect(showNewMessage).not.toHaveBeenCalled();
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
