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
import { addDays, subDays } from "date-fns";
import React from "react";

import { licenseKeysQueryKeys } from "~/access/license/queries";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { useAppConfig } from "~/config/useAppConfig";
import { isSuperUserQueryKeys } from "~/hooks/useIsSuperUser";
import { getQueryClient } from "~/queryClient";
import { renderComponent } from "~/test/utils";

import {
  LICENSE_EXPIRED_DISMISSED_KEY,
  LicenseExpiredBanner,
} from "./LicenseExpiredBanner";

vi.mock("~/config/useAppConfig");

const setAppMode = (mode: "cloud" | "self-managed") => {
  vi.mocked(useAppConfig).mockReturnValue({ mode } as ReturnType<
    typeof useAppConfig
  >);
};

const buildIsSuperUserHandler = (isSuperUser: boolean) =>
  buildSqlQueryHandlerV2({
    queryKey: isSuperUserQueryKeys.isSuperUser(),
    results: mapKyselyToTabular({
      columns: [buildColumn({ name: "isSuperUser" })],
      rows: [{ isSuperUser }],
    }),
  });

const buildLicenseKeyHandler = (expiration: string | null) =>
  buildSqlQueryHandlerV2({
    queryKey: licenseKeysQueryKeys.detail(),
    results: mapKyselyToTabular({
      columns: [
        buildColumn({ name: "environment_id" }),
        buildColumn({ name: "expiration" }),
        buildColumn({ name: "id" }),
        buildColumn({ name: "not_before" }),
        buildColumn({ name: "organization" }),
      ],
      rows:
        expiration === null
          ? []
          : [
              {
                environment_id: "u1",
                expiration,
                id: "license-1",
                not_before: subDays(new Date(), 365).toISOString(),
                organization: "acme-corp",
              },
            ],
    }),
  });

const EXPIRED_DATE = () => subDays(new Date(), 1).toISOString();
const ACTIVE_DATE = () => addDays(new Date(), 30).toISOString();

describe("LicenseExpiredBanner", () => {
  beforeEach(() => {
    localStorage.clear();
    setAppMode("self-managed");
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  it("renders when self-managed, the user is a super user, and the license has expired", async () => {
    server.use(buildIsSuperUserHandler(true));
    server.use(buildLicenseKeyHandler(EXPIRED_DATE()));

    renderComponent(<LicenseExpiredBanner />);

    expect(await screen.findByTestId("license-expired-alert")).toBeVisible();
    expect(
      screen.getByText(/Your Materialize Enterprise license has expired\./i),
    ).toBeVisible();
    const supportLink = screen.getByRole("link", { name: "Contact support" });
    expect(supportLink).toBeVisible();
    expect(supportLink).toHaveAttribute(
      "href",
      "https://materialize.com/s/chat",
    );
  });

  it("does not render when the app is not self-managed", async () => {
    setAppMode("cloud");

    renderComponent(<LicenseExpiredBanner />);

    // In cloud mode the inner content (and its SQL queries) never mounts, so we
    // can assert absence directly.
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });

  it("does not render when the user is not a super user", async () => {
    server.use(buildIsSuperUserHandler(false));
    server.use(buildLicenseKeyHandler(EXPIRED_DATE()));

    renderComponent(<LicenseExpiredBanner />);

    // Wait for the super user query to resolve, then assert the banner stays hidden.
    await waitFor(() =>
      expect(
        getQueryClient().getQueryData(isSuperUserQueryKeys.isSuperUser()),
      ).toBe(false),
    );
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });

  it("does not render when the license has not expired", async () => {
    server.use(buildIsSuperUserHandler(true));
    server.use(buildLicenseKeyHandler(ACTIVE_DATE()));

    renderComponent(<LicenseExpiredBanner />);

    // Wait for both queries to resolve, then assert the banner stays hidden.
    await waitFor(() =>
      expect(
        getQueryClient().getQueryData(isSuperUserQueryKeys.isSuperUser()),
      ).toBe(true),
    );
    await waitFor(() =>
      expect(
        getQueryClient().getQueryData(licenseKeysQueryKeys.detail()),
      ).toBeDefined(),
    );
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });

  it("does not render when there is no license key", async () => {
    server.use(buildIsSuperUserHandler(true));
    server.use(buildLicenseKeyHandler(null));

    renderComponent(<LicenseExpiredBanner />);

    await waitFor(() =>
      expect(
        getQueryClient().getQueryData(licenseKeysQueryKeys.detail()),
      ).toBeDefined(),
    );
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });

  it("hides the banner after the close button is clicked", async () => {
    const user = userEvent.setup();
    server.use(buildIsSuperUserHandler(true));
    server.use(buildLicenseKeyHandler(EXPIRED_DATE()));

    renderComponent(<LicenseExpiredBanner />);

    expect(await screen.findByTestId("license-expired-alert")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Close" }));
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });

  it("does not render when it has already been dismissed", async () => {
    localStorage.setItem(LICENSE_EXPIRED_DISMISSED_KEY, JSON.stringify(true));
    server.use(buildIsSuperUserHandler(true));
    server.use(buildLicenseKeyHandler(EXPIRED_DATE()));

    renderComponent(<LicenseExpiredBanner />);

    await waitFor(() =>
      expect(
        getQueryClient().getQueryData(licenseKeysQueryKeys.detail()),
      ).toBeDefined(),
    );
    expect(
      screen.queryByTestId("license-expired-alert"),
    ).not.toBeInTheDocument();
  });
});
