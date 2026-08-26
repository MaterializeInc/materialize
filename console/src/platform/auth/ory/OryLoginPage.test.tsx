// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import React from "react";

import {
  createBrowserLoginFlow,
  dummyIdentifierFirstLoginFlow,
  dummyLoginFlow,
  dummyMultiProviderLoginFlow,
  dummyPasswordLoginFlow,
  getLoginFlow,
  MOCK_CSRF_TOKEN,
  MOCK_FLOW_ID,
} from "~/external-library-wrappers/__mocks__/ory";
import { ResponseError } from "~/external-library-wrappers/ory";
import { renderComponent } from "~/test/utils";

import { resetOryClient } from "./oryConfig";
import { OryLoginPage } from "./OryLoginPage";

describe("OryLoginPage", () => {
  let submitted: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.stubEnv("VITE_ORY_SDK_URL", "http://localhost:4000");
    // The client is memoized across calls, so it has to be discarded or the
    // first test's resolved URL leaks into every later one.
    resetOryClient();
    createBrowserLoginFlow.mockResolvedValue(dummyLoginFlow);
    getLoginFlow.mockResolvedValue(dummyLoginFlow);
    // jsdom does not navigate, so the submit is observed and cancelled.
    submitted = vi.fn((e: SubmitEvent) => e.preventDefault());
    window.addEventListener("submit", submitted);
  });

  afterEach(() => {
    window.removeEventListener("submit", submitted);
    vi.unstubAllEnvs();
    vi.clearAllMocks();
  });

  it("renders a button per provider the flow offers", async () => {
    createBrowserLoginFlow.mockResolvedValue(dummyMultiProviderLoginFlow);

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    const auth0 = await screen.findByRole("button", {
      name: "Sign in with Auth0",
    });
    expect(auth0).toHaveAttribute("name", "provider");
    expect(auth0).toHaveAttribute("value", "auth0");
    // A federated button carries a mark, so it does not read as plain text.
    expect(auth0.querySelector("svg")).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: "Sign in with Okta" }),
    ).toHaveAttribute("value", "okta");
  });

  // Kratos answers the submit with a redirect to the provider, so the form has
  // to post to Kratos itself rather than fetch in the background.
  it("posts to the action Kratos supplied, replaying the CSRF token", async () => {
    createBrowserLoginFlow.mockResolvedValue(dummyMultiProviderLoginFlow);

    const { container } = await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    await screen.findByRole("button", { name: "Sign in with Auth0" });
    const form = container.querySelector("form");
    expect(form).toHaveAttribute("action", dummyLoginFlow.ui.action);
    expect(form).toHaveAttribute("method", "POST");
    expect(container.querySelector('input[name="csrf_token"]')).toHaveValue(
      MOCK_CSRF_TOKEN,
    );
  });

  it("reuses the flow already in the URL", async () => {
    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: [`/account/login?flow=${MOCK_FLOW_ID}`],
    });

    await waitFor(() => expect(getLoginFlow).toHaveBeenCalled());
    expect(createBrowserLoginFlow).not.toHaveBeenCalled();
  });

  it("forwards the Hydra login challenge so Kratos can accept it", async () => {
    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login?login_challenge=challenge-abc"],
    });

    await waitFor(() =>
      expect(createBrowserLoginFlow).toHaveBeenCalledWith(
        expect.objectContaining({ loginChallenge: "challenge-abc" }),
      ),
    );
  });

  it("starts a new flow when the one in the URL has expired", async () => {
    getLoginFlow.mockRejectedValueOnce(
      new ResponseError(new Response(null, { status: 410 })),
    );

    const { container } = await renderComponent(<OryLoginPage />, {
      initialRouterEntries: [`/account/login?flow=${MOCK_FLOW_ID}`],
    });

    await waitFor(() => expect(createBrowserLoginFlow).toHaveBeenCalled());
    expect(container.querySelector("form")).toHaveAttribute(
      "action",
      dummyLoginFlow.ui.action,
    );
  });

  // Kratos returns a chooser even for one provider; clicking through it is
  // friction an ordinary SSO sign-in does not have.
  it("submits straight to the provider when the flow offers only one", async () => {
    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    await waitFor(() => expect(submitted).toHaveBeenCalled());
    expect(
      (submitted.mock.calls[0][0].submitter as HTMLButtonElement).value,
    ).toBe("auth0");
  });

  it("waits for the user when the flow carries a message", async () => {
    createBrowserLoginFlow.mockResolvedValue({
      ...dummyLoginFlow,
      ui: {
        ...dummyLoginFlow.ui,
        messages: [
          { id: 4000006, text: "The provider rejected you", type: "error" },
        ],
      },
    });

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    expect(await screen.findByText("The provider rejected you")).toBeVisible();
    expect(submitted).not.toHaveBeenCalled();
  });

  // Being signed in already is not a failure; Kratos wants the flow restarted
  // as a refresh so the person reconfirms rather than being told they cannot.
  it("retries as a refresh when a session already exists", async () => {
    createBrowserLoginFlow.mockRejectedValueOnce(
      new ResponseError(
        new Response(
          JSON.stringify({ error: { id: "session_already_available" } }),
          { status: 400 },
        ),
      ),
    );

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    await waitFor(() =>
      expect(createBrowserLoginFlow).toHaveBeenLastCalledWith(
        expect.objectContaining({ refresh: true }),
      ),
    );
    expect(screen.queryByText(/valid session/)).not.toBeInTheDocument();
    await waitFor(() => expect(submitted).toHaveBeenCalled());
  });

  // Hydra hands back a refresh flow whenever a Kratos session already exists.
  // Stopping there would put a button in front of every OAuth2 sign-in, and
  // pressing it verifies nothing the provider is not about to verify anyway.
  it("continues through a refresh flow rather than stopping to confirm", async () => {
    createBrowserLoginFlow.mockResolvedValue({
      ...dummyLoginFlow,
      refresh: true,
      ui: {
        ...dummyLoginFlow.ui,
        messages: [
          { id: 1010003, text: "Please confirm this action", type: "info" },
        ],
      },
    });

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login?login_challenge=challenge-abc"],
    });

    await waitFor(() => expect(submitted).toHaveBeenCalled());
  });

  // Identifier-first asks for an email before naming a provider, so the form has
  // to draw a text field and wait for it rather than redirecting.
  it("draws the identifier field and waits for input", async () => {
    createBrowserLoginFlow.mockResolvedValue(dummyIdentifierFirstLoginFlow);

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    const field = await screen.findByLabelText(/^ID/);
    expect(field).toHaveAttribute("name", "identifier");
    expect(field).toBeRequired();
    expect(
      screen.getByRole("button", { name: "Continue" }),
    ).toBeInTheDocument();
    expect(submitted).not.toHaveBeenCalled();
  });

  // Advancing the form and handing off to a provider are different actions, so
  // they must not read as two equal choices.
  it("ranks the step's own submit above the provider buttons", async () => {
    createBrowserLoginFlow.mockResolvedValue({
      ...dummyIdentifierFirstLoginFlow,
      ui: {
        ...dummyIdentifierFirstLoginFlow.ui,
        nodes: [
          ...dummyIdentifierFirstLoginFlow.ui.nodes,
          dummyLoginFlow.ui.nodes[0],
        ],
      },
    });

    const { container } = await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    await screen.findByRole("button", { name: "Continue" });
    // Scoped to the form: AuthLayout renders a copy button of its own.
    const buttons = Array.from(container.querySelectorAll("form button"));
    expect(buttons.map((b) => b.textContent)).toEqual([
      "Continue",
      "Sign in with Auth0",
    ]);
    expect(buttons[0].className).not.toEqual(buttons[1].className);
    expect(screen.getByText("or")).toBeVisible();
  });

  // Kratos attaches a script node to flows that use WebAuthn. It decorates the
  // flow; the method that needs it contributes its own input, which is what
  // should decide whether this page can draw the form.
  it("does not warn about presentational nodes it skips", async () => {
    createBrowserLoginFlow.mockResolvedValue({
      ...dummyLoginFlow,
      ui: {
        ...dummyLoginFlow.ui,
        nodes: [
          ...dummyLoginFlow.ui.nodes,
          {
            type: "script",
            group: "webauthn",
            attributes: {
              id: "webauthn_script",
              src: "https://example.com/webauthn.js",
              type: "text/javascript",
              async: true,
              referrerpolicy: "no-referrer",
              crossorigin: "anonymous",
              integrity: "sha512-x",
              nonce: "",
              node_type: "script",
            },
            messages: [],
            meta: {},
          },
        ],
      },
    });

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    // A regex, not the exact name: the button submits itself here, and Chakra's
    // spinner adds a visually hidden "Loading..." to its accessible name.
    await screen.findByRole("button", { name: /Sign in with Auth0/ });
    expect(screen.queryByText(/does not support/)).not.toBeInTheDocument();
  });

  // An image carries something the user has to read, a TOTP QR code say, so
  // dropping it leaves a form that cannot be completed.
  it("warns when it skips a node the user needs to see", async () => {
    createBrowserLoginFlow.mockResolvedValue({
      ...dummyLoginFlow,
      ui: {
        ...dummyLoginFlow.ui,
        nodes: [
          ...dummyLoginFlow.ui.nodes,
          {
            type: "img",
            group: "totp",
            attributes: {
              id: "totp_qr",
              src: "data:image/png;base64,x",
              width: 256,
              height: 256,
              node_type: "img",
            },
            messages: [],
            meta: {},
          },
        ],
      },
    });

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    expect(await screen.findByText(/does not support/)).toBeVisible();
  });

  // Status codes are an implementation detail; the reader can do nothing with
  // one, and CLAUDE.md rules them out of user-facing copy.
  it("keeps the transport out of the message when Ory says nothing useful", async () => {
    createBrowserLoginFlow.mockRejectedValueOnce(
      new ResponseError(new Response("", { status: 500 })),
    );

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    expect(
      await screen.findByText(
        "Sign-in could not be started. Please try again.",
      ),
    ).toBeVisible();
    expect(screen.queryByText(/500/)).not.toBeInTheDocument();
  });

  it("reports a sign-in method it cannot render instead of drawing a dead form", async () => {
    createBrowserLoginFlow.mockResolvedValue(dummyPasswordLoginFlow);

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    expect(
      await screen.findByText(/sign-in method this page does not support/),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /sign in/i }),
    ).not.toBeInTheDocument();
  });

  it("surfaces an error when the flow cannot be started", async () => {
    createBrowserLoginFlow.mockRejectedValueOnce(new Error("Ory unreachable"));

    await renderComponent(<OryLoginPage />, {
      initialRouterEntries: ["/account/login"],
    });

    await waitFor(async () =>
      expect(await screen.findByText("Ory unreachable")).toBeVisible(),
    );
  });
});
