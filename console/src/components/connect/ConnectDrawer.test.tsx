// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import { renderComponent } from "~/test/utils";

import ConnectDrawer from "./ConnectDrawer";

// Matches `healthyEnvironment` in test/utils.tsx.
const ENVIRONMENT_HOST = "8zpze6ltqnsjok9vvf2i99st5.us-east-1.aws.example.com";

const renderDrawer = () =>
  renderComponent(
    <ConnectDrawer isOpen onClose={vi.fn()} user={dummyValidUser} />,
  );

describe("ConnectDrawer", () => {
  it("shows the MCP server instructions by default", async () => {
    await renderDrawer();

    expect(await screen.findByText("Choose your MCP server")).toBeVisible();
    // The test environment predates the OAuth gate, so the token flow shows.
    expect(
      screen.getByRole("button", { name: "Generate personal MCP token" }),
    ).toBeVisible();
    expect(
      screen.getByText(new RegExp(`https://${ENVIRONMENT_HOST}`)),
    ).toBeVisible();
    expect(screen.getByText(/materialize-developer/)).toBeVisible();
    expect(screen.getByText("Install agent skills (optional)")).toBeVisible();
  });

  it("switches MCP config format per client", async () => {
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "MCP client" }));
    await user.click(await screen.findByRole("menuitem", { name: "Cursor" }));

    expect(await screen.findByText("Add to ~/.cursor/mcp.json:")).toBeVisible();
    expect(screen.getByText(/mcpServers/)).toBeVisible();
  });

  it("switches to the agent MCP server", async () => {
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(await screen.findByText("Query your data products"));

    expect(await screen.findByText(/materialize-agent/)).toBeVisible();
    expect(screen.getByText(/Admins can scope agent access/)).toBeVisible();
    expect(
      screen.queryByText("Install agent skills (optional)"),
    ).not.toBeInTheDocument();
  });

  it("shows connection details on the External tools tab", async () => {
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(
      await screen.findByRole("button", { name: /External tools/ }),
    );

    expect(await screen.findByLabelText("Host")).toHaveTextContent(
      ENVIRONMENT_HOST,
    );
    expect(screen.getByLabelText("Port")).toHaveTextContent("6875");
    expect(screen.getByLabelText("Database")).toHaveTextContent("materialize");
    expect(screen.getByLabelText("User")).toHaveTextContent(
      dummyValidUser.email,
    );
    expect(
      screen.getByText(/In DBeaver, create a new PostgreSQL/),
    ).toBeVisible();
  });

  it("creates an app password and substitutes it into tool snippets", async () => {
    server.use(
      http.post("*/frontegg/identity/resources/users/api-tokens/v1", () =>
        HttpResponse.json({
          clientId: "11111111-1111-1111-1111-111111111111",
          secret: "22222222-2222-2222-2222-222222222222",
          createdAt: "2026-01-01T00:00:00Z",
          description: "External tools",
          metadata: {},
        }),
      ),
    );
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(
      await screen.findByRole("button", { name: /External tools/ }),
    );
    await user.click(
      await screen.findByRole("button", { name: "Create app password" }),
    );
    const nameInput = await screen.findByLabelText("App password name");
    expect(nameInput).toHaveValue("External tools");
    await user.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(
        screen.getByText(/Copy it now\. It will not be shown again\./),
      ).toBeVisible();
    });
    const expectedPassword = `mzp_${"1".repeat(32)}${"2".repeat(32)}`;
    expect(
      screen.getByText(new RegExp(`Password\\s+${expectedPassword}`)),
    ).toBeVisible();
  });

  it("shows the psql command on the Terminal tab", async () => {
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: /Terminal/ }));

    expect(
      await screen.findByText(
        new RegExp(
          `psql "postgres://${encodeURIComponent(dummyValidUser.email)}@${ENVIRONMENT_HOST}:6875/materialize\\?sslmode=require"`,
        ),
      ),
    ).toBeVisible();
  });
});
