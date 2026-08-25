// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import { UserApiToken } from "~/api/frontegg/types";
import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import { renderComponent } from "~/test/utils";

import AppPasswordsPage from "./AppPasswordsPage";

const DAY_MS = 24 * 60 * 60 * 1000;

const buildToken = (props: Partial<UserApiToken>): UserApiToken => ({
  type: "personal",
  clientId: "11111111-1111-1111-1111-111111111111",
  createdAt: "2026-01-01T00:00:00Z",
  description: "Personal laptop",
  metadata: {},
  ...props,
});

const ROLE = { id: "role-id", key: "Admin", name: "Admin" };

/** Stubs every request the page makes, plus both create endpoints, and returns
 * the body of the last create request against each. */
const mockFrontegg = (tokens: UserApiToken[]) => {
  const created: {
    personal?: Record<string, unknown>;
    service?: Record<string, unknown>;
  } = {};
  const capture =
    (kind: "personal" | "service") =>
    async ({ request }: { request: Request }) => {
      created[kind] = (await request.json()) as Record<string, unknown>;
      return HttpResponse.json({
        ...buildToken({ clientId: "22222222-2222-2222-2222-222222222222" }),
        secret: "33333333-3333-3333-3333-333333333333",
      });
    };
  server.use(
    http.get("*/frontegg/identity/resources/users/api-tokens/v1", () =>
      HttpResponse.json(tokens),
    ),
    http.get("*/frontegg/identity/resources/tenants/api-tokens/v1", () =>
      HttpResponse.json([]),
    ),
    http.get("*/frontegg/team/resources/roles/v1", () =>
      HttpResponse.json({ items: [ROLE] }),
    ),
    http.post(
      "*/frontegg/identity/resources/users/api-tokens/v1",
      capture("personal"),
    ),
    http.post(
      "*/frontegg/identity/resources/tenants/api-tokens/v1",
      capture("service"),
    ),
  );
  return created;
};

const renderPage = (openNewModal = false) =>
  renderComponent(<AppPasswordsPage user={dummyValidUser} />, {
    initialRouterEntries: openNewModal
      ? [{ pathname: "/", state: { new: true } }]
      : ["/"],
  });

const findRow = (description: string) =>
  screen.findByRole("row", { name: description });

describe("AppPasswordsPage", () => {
  it("renders passwords without an expiration as never expiring", async () => {
    mockFrontegg([buildToken({ description: "Legacy password" })]);
    await renderPage();

    expect(
      within(await findRow("Legacy password")).getByText("Never"),
    ).toBeVisible();
  });

  it("flags expired and soon to expire passwords", async () => {
    mockFrontegg([
      buildToken({
        clientId: "aaaaaaaa-1111-1111-1111-111111111111",
        description: "Stale password",
        expires: new Date(Date.now() - DAY_MS).toISOString(),
      }),
      buildToken({
        clientId: "bbbbbbbb-1111-1111-1111-111111111111",
        description: "Almost stale password",
        expires: new Date(Date.now() + 3 * DAY_MS).toISOString(),
      }),
      buildToken({
        clientId: "cccccccc-1111-1111-1111-111111111111",
        description: "Fresh password",
        expires: new Date(Date.now() + 30 * DAY_MS).toISOString(),
      }),
    ]);
    await renderPage();

    expect(
      within(await findRow("Stale password")).getByText("Expired"),
    ).toBeVisible();
    expect(
      within(await findRow("Almost stale password")).getByText("Expiring soon"),
    ).toBeVisible();
    const freshRow = within(await findRow("Fresh password"));
    expect(freshRow.queryByText("Expired")).not.toBeInTheDocument();
    expect(freshRow.queryByText("Expiring soon")).not.toBeInTheDocument();
  });

  it("defaults new passwords to a 90 day expiration", async () => {
    const created = mockFrontegg([]);
    await renderPage(true);
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "New password");
    await user.click(screen.getByRole("button", { name: "Create Password" }));

    await waitFor(() =>
      expect(created.personal).toMatchObject({
        description: "New password",
        expiresInMinutes: 90 * 24 * 60,
      }),
    );
  });

  it("omits the expiration when no expiration is selected", async () => {
    const created = mockFrontegg([]);
    await renderPage(true);
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "New password");
    await user.selectOptions(screen.getByLabelText("Expiration"), "never");
    await user.click(screen.getByRole("button", { name: "Create Password" }));

    await waitFor(() => expect(created.personal).toBeDefined());
    expect(created.personal).not.toHaveProperty("expiresInMinutes");
  });

  it("sends the expiration on the service password endpoint too", async () => {
    const created = mockFrontegg([]);
    await renderPage(true);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("radio", { name: /Service/ }));
    await user.type(screen.getByLabelText("Name"), "Service password");
    // The User field's label is not wired to its input, so go by position.
    await user.type(screen.getAllByRole("textbox")[1], "svc");
    await user.click(screen.getByPlaceholderText("Select..."));
    await user.click(await screen.findByRole("option", { name: ROLE.name }));
    await user.selectOptions(screen.getByLabelText("Expiration"), "30d");
    await user.click(screen.getByRole("button", { name: "Create Password" }));

    await waitFor(() =>
      expect(created.service).toMatchObject({
        description: "Service password",
        metadata: { user: "svc" },
        roleIds: [ROLE.id],
        expiresInMinutes: 30 * 24 * 60,
      }),
    );
  });
});
