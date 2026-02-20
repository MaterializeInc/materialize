// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ErrorBoundary } from "@sentry/react";
import { screen } from "@testing-library/dom";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import { STATUSPAGE_URL } from "~/api/incident-io";
import {
  Incident,
  InProgressMaintenance,
  ScheduledMaintenance,
  StatusSummary,
} from "~/api/incident-io/types";
import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";

import IncidentStatusWidget from "./IncidentStatusWidget";

function getIncident(): Incident {
  return {
    id: "fakeid",
    url: "https://mz.example.com/fakeid",
    name: "Inflammable means flammable",
    status: "investigating",
    current_worst_impact: "partial_outage",
    last_update_at: "2023-12-12T00:00:00Z",
    affected_components: [],
    last_update_message: "What a country!",
  };
}

function buildStatusResponse(
  options: {
    incidents?: Incident[];
    in_progress_maintenances?: InProgressMaintenance[];
    scheduled_maintenances?: ScheduledMaintenance[];
  } = {},
) {
  return http.get(STATUSPAGE_URL, () => {
    const response: StatusSummary = {
      page_title: "Materialize Status",
      page_url: "https://mz.example.com",
      ongoing_incidents: options.incidents ?? [],
      in_progress_maintenances: options.in_progress_maintenances ?? [],
      scheduled_maintenances: options.scheduled_maintenances ?? [],
    };
    return HttpResponse.json(response);
  });
}

describe("IncidentStatusWidget", () => {
  it("displays a toast", async () => {
    const incident = getIncident();
    server.use(buildStatusResponse({ incidents: [incident] }));
    renderComponent(<IncidentStatusWidget />);
    expect(await screen.findByText(incident.name)).toBeVisible();
  });

  it("displays multiple toasts", async () => {
    const incident1 = {
      ...getIncident(),
      name: "Incident 1",
      id: "incident-1",
    };
    const incident2 = {
      ...getIncident(),
      name: "Incident 2",
      id: "incident-2",
    };
    server.use(buildStatusResponse({ incidents: [incident1, incident2] }));
    renderComponent(<IncidentStatusWidget />);
    expect(await screen.findByText(incident1.name)).toBeVisible();
    expect(await screen.findByText(incident2.name)).toBeVisible();
  });

  it("closing a toast dismisses it", async () => {
    const user = userEvent.setup();
    const incident = getIncident();
    server.use(buildStatusResponse({ incidents: [incident] }));
    renderComponent(<IncidentStatusWidget />);
    expect(await screen.findByText(incident.name)).toBeVisible();
    const closeButton = screen.getByLabelText("Close");
    await user.click(closeButton);
    expect(screen.queryByText(incident.name)).not.toBeInTheDocument();
  });

  it("should silently fail and be guarded by the error boundary if the statuspage API call returns an error", async () => {
    server.use(
      http.get(STATUSPAGE_URL, () => {
        return HttpResponse.json(
          "JSON.parse: unexpected end of data at line 1 column 1 of the JSON data",
          { status: 500 },
        );
      }),
    );

    await renderComponent(
      <ErrorBoundary fallback={<p>Error was not guarded</p>}>
        <IncidentStatusWidget />
      </ErrorBoundary>,
    );

    // Asserts that Testing Library's findByText won't be able to find the text
    expect(
      (async () => {
        await screen.findByText("Error was not guarded", undefined, {
          timeout: 1000,
        });
      })(),
    ).rejects.toThrowError();
  });
});
