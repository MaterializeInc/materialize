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
import {
  defaultRegionId,
  disabledEnvironment,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

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
    expect(screen.getByText("Select your client")).toBeVisible();
    // The test environment predates the OAuth gate, so the token flow shows.
    expect(
      screen.getByRole("button", { name: "Generate app password" }),
    ).toBeVisible();
    expect(screen.getByText("Add the MCP server")).toBeVisible();
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

    expect(await screen.findByText("Add to ~/.cursor/mcp.json")).toBeVisible();
    expect(screen.getByText(/mcpServers/)).toBeVisible();
  });

  it("switches to the agent MCP server", async () => {
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(await screen.findByText("Query your data products"));

    expect(await screen.findByText(/materialize-agent/)).toBeVisible();
    expect(
      screen.queryByText("Install agent skills (optional)"),
    ).not.toBeInTheDocument();
  });

  it("reports that a disabled region cannot serve connection details", async () => {
    await renderComponent(
      <ConnectDrawer isOpen onClose={vi.fn()} user={dummyValidUser} />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, defaultRegionId, disabledEnvironment),
      },
    );

    expect(
      await screen.findByText(/Connection details are unavailable/),
    ).toBeVisible();
    expect(
      screen.queryByText("Choose your MCP server"),
    ).not.toBeInTheDocument();
  });

  it("shows the OAuth flow with an authenticate step on gated environments", async () => {
    await renderComponent(
      <ConnectDrawer isOpen onClose={vi.fn()} user={dummyValidUser} />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, defaultRegionId, {
            ...healthyEnvironment,
            status: {
              health: "healthy",
              errors: [],
              version: parseDbVersion("v26.30.0 (abcdef123)"),
            },
          }),
      },
    );
    const user = userEvent.setup();

    expect(await screen.findByText("Authenticate")).toBeVisible();
    expect(screen.getByText("claude /mcp")).toBeVisible();
    expect(
      screen.getByText(/Select the materialize-developer server/),
    ).toBeVisible();
    // The OAuth command carries no credential.
    expect(screen.queryByText(/Authorization/)).not.toBeInTheDocument();

    await user.click(screen.getByText("Use a token instead"));

    expect(
      await screen.findByRole("button", { name: "Generate app password" }),
    ).toBeVisible();
    expect(screen.queryByText("Authenticate")).not.toBeInTheDocument();
    expect(screen.getByText("Use OAuth instead")).toBeVisible();
  });

  it("generates an MCP token and masks it in the command", async () => {
    server.use(
      http.post("*/frontegg/identity/resources/users/api-tokens/v1", () =>
        HttpResponse.json({
          clientId: "11111111-1111-1111-1111-111111111111",
          secret: "22222222-2222-2222-2222-222222222222",
          createdAt: "2026-01-01T00:00:00Z",
          description: "MCP token",
          metadata: {},
        }),
      ),
    );
    await renderDrawer();
    const user = userEvent.setup();

    await user.click(
      await screen.findByRole("button", { name: "Generate app password" }),
    );

    expect(
      await screen.findByText(/App password created and added to the command/),
    ).toBeVisible();
    expect(screen.getByText(/Authorization: Basic \*+/)).toBeVisible();
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
      await screen.findByRole("button", { name: "Create new password" }),
    );
    const nameInput = await screen.findByLabelText("App password name");
    expect(nameInput).toHaveValue("");
    expect(screen.getByText(/You are naming this password/)).toBeVisible();
    expect(screen.getByRole("button", { name: "Create" })).toBeDisabled();
    await user.type(nameInput, "External tools");
    await user.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(
        screen.getByText(/Copy it now\. It will not be shown again\./),
      ).toBeVisible();
    });
    // The tool snippet shows the password masked. Copying yields the real one.
    const expectedPassword = `mzp_${"1".repeat(32)}${"2".repeat(32)}`;
    expect(
      screen.getByText(
        new RegExp(`Password\\s+\\*{${expectedPassword.length}}`),
      ),
    ).toBeVisible();
    expect(
      screen.queryByText(new RegExp(expectedPassword)),
    ).not.toBeInTheDocument();
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
