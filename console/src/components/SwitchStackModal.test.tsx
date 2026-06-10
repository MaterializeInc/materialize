// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import React from "react";

import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";

import SwitchStackModal from "./SwitchStackModal";

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => {
    return { "switch-stacks-modal": true };
  },
}));

function getSwitchStackButton() {
  return screen.getByRole("button", { name: "Switch stack" });
}

describe("SwitchStackModal", () => {
  beforeEach(() => {
    Object.defineProperty(window, "location", {
      writable: true,
      value: { assign: vi.fn() },
    });
    window.location.hostname = "localhost";
  });

  describe("when the feature flag is enabled", () => {
    it("shows the switch stack button", async () => {
      await renderComponent(<SwitchStackModal />);

      expect(getSwitchStackButton()).toBeVisible();
    });

    it("shows the current stack", async () => {
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: test")).toBeVisible(),
      );
    });

    it("shows stack options", async () => {
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: test")).toBeVisible(),
      );
      expect(screen.getByText("Staging")).toBeVisible();
      expect(screen.getByText("Local")).toBeVisible();
      expect(screen.getByText("Personal")).toBeVisible();
      // Production doesn't show up on localhost, since it won't work
      expect(screen.queryByText("Production")).not.toBeInTheDocument();
    });

    it("does not show production option on staging", async () => {
      window.location.hostname = "staging.console.materialize.com";
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: staging")).toBeVisible(),
      );
      expect(screen.getByLabelText("Staging")).toBeVisible();
      expect(screen.queryByText("Production")).not.toBeInTheDocument();
    });

    it("shows the production stack on production urls", async () => {
      window.location.hostname = "console.materialize.com";
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: test")).toBeVisible(),
      );
      expect(screen.getByLabelText("Production")).toBeVisible();
    });

    it("shows and error message for invalid personal stacks", async () => {
      server.use(
        http.get("https://admin.fake.dev.cloud.materialize.com/*", () => {
          throw new Error("address not found");
        }),
      );
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: test")).toBeVisible(),
      );
      fireEvent.change(screen.getByPlaceholderText("$USER.$ENV"), {
        target: {
          value: "fake.dev",
        },
      });
      fireEvent.click(screen.getByText("Switch"));
      expect(
        await screen.findByText(
          "https://admin.fake.dev.cloud.materialize.com is not reachable from this origin.",
        ),
      ).toBeVisible();
    });

    it("switches to valid personal stack", async () => {
      location.reload = vi.fn();
      server.use(
        http.get("https://admin.someuser.dev.cloud.materialize.com/*", () => {
          return new HttpResponse();
        }),
      );
      await renderComponent(<SwitchStackModal />);

      fireEvent.click(getSwitchStackButton());
      await waitFor(() =>
        expect(screen.getByText("Current Stack: test")).toBeVisible(),
      );
      fireEvent.change(screen.getByPlaceholderText("$USER.$ENV"), {
        target: {
          value: "someuser.dev",
        },
      });
      fireEvent.click(screen.getByText("Switch"));
      await waitFor(() => {
        expect(window.localStorage.getItem("mz-current-stack")).toEqual(
          "someuser.dev",
        );
      });
      expect(location.reload).toHaveBeenCalled();
    });
  });
});
