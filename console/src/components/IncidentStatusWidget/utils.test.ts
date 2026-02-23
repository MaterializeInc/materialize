// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addDays, addHours } from "date-fns";

import {
  Incident,
  InProgressMaintenance,
  ScheduledMaintenance,
  StatusSummary,
} from "~/api/incident-io/types";
import { nowUTC } from "~/util";

import { getHeadlineEvents, isInRegion, LastDismissedEvents } from "./utils";

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

function getInProgressMaintenance(): InProgressMaintenance {
  return {
    id: "fakeid",
    url: "https://mz.example.com/fakeid",
    name: "Scheduled window",
    status: "maintenance_in_progress",
    scheduled_end_at: "2024-01-02T00:00:00Z",
    started_at: "2024-01-01T00:00:00Z",
    last_update_at: "2023-12-12T00:00:00Z",
    affected_components: [],
    last_update_message: "Created",
  };
}

function getScheduledMaintenance(): ScheduledMaintenance {
  return {
    id: "fakeid",
    url: "https://mz.example.com/fakeid",
    name: "Scheduled window",
    status: "maintenance_scheduled",
    ends_at: "2024-01-02T00:00:00Z",
    starts_at: "2024-01-01T00:00:00Z",
    last_update_at: "2023-12-12T00:00:00Z",
    affected_components: [],
    last_update_message: "Created",
  };
}

function getEmptyStatusSummary(): StatusSummary {
  return {
    page_url: "https://mz.example.com",
    page_title: "Materialize Incidents",
    ongoing_incidents: [],
    in_progress_maintenances: [],
    scheduled_maintenances: [],
  };
}

function getEmptyDismissedEvents(): LastDismissedEvents {
  return {
    ongoing_incident: [],
    in_progress_maintenance: [],
    scheduled_maintenance: [],
  };
}

describe("IncidentStatusWidget utils", () => {
  it("can determine whether or not an event is in the filtered region", () => {
    const region = "aws/us-east-1";

    const without = getScheduledMaintenance();
    expect(isInRegion(without, region)).toBeTruthy();

    const affected = {
      ...without,
      affected_components: [
        {
          name: region,
          group_name: "Cloud regions",
          id: "cloud-id",
        },
      ],
    };
    expect(isInRegion(affected, region)).toBeTruthy();

    const unaffected = {
      ...without,
      affected_components: [
        {
          name: "aws/eu-west-1",
          group_name: "Cloud regions",
          id: "cloud-id",
        },
      ],
    };
    expect(isInRegion(unaffected, region)).toBeFalsy();
    expect(isInRegion(unaffected, "")).toBeTruthy();

    const multiple = {
      ...without,
      affected_components: [
        {
          name: "aws/eu-west-1",
          group_name: "Cloud regions",
          id: "cloud-id-1",
        },
        {
          name: region,
          group_name: "Cloud regions",
          id: "cloud-id-2",
        },
        {
          name: "aws/us-west-2",
          group_name: "Cloud regions",
          id: "cloud-id-3",
        },
      ],
    };
    expect(isInRegion(multiple, region)).toBeTruthy();
  });

  it("prioritizes incidents over in-progress and scheduled maintenances", () => {
    const summary = {
      ...getEmptyStatusSummary(),
      ongoing_incidents: [getIncident()],
      in_progress_maintenances: [getInProgressMaintenance()],
      scheduled_maintenances: [getScheduledMaintenance()],
    };
    expect(getHeadlineEvents(summary, getEmptyDismissedEvents(), "")).toEqual(
      summary.ongoing_incidents,
    );
  });

  it("prioritizes in-progress maintenance over scheduled maintenance", () => {
    const summary = {
      ...getEmptyStatusSummary(),
      ongoing_incidents: [],
      in_progress_maintenances: [getInProgressMaintenance()],
      scheduled_maintenances: [getScheduledMaintenance()],
    };
    expect(getHeadlineEvents(summary, getEmptyDismissedEvents(), "")).toEqual(
      summary.in_progress_maintenances,
    );
  });

  it("excludes dismissed events", () => {
    const summary = {
      ...getEmptyStatusSummary(),
      ongoing_incidents: [],
      in_progress_maintenances: [getInProgressMaintenance()],
      scheduled_maintenances: [],
    };
    const dismissed = getEmptyDismissedEvents();
    dismissed.in_progress_maintenance.push(
      summary.in_progress_maintenances[0].id,
    );
    expect(getHeadlineEvents(summary, dismissed, "")).toHaveLength(0);
  });

  it("includes dismissed scheduled maintenances that become in-progress", () => {
    const summary = {
      ...getEmptyStatusSummary(),
      ongoing_incidents: [],
      in_progress_maintenances: [getInProgressMaintenance()],
      scheduled_maintenances: [],
    };
    const dismissed = getEmptyDismissedEvents();
    dismissed.scheduled_maintenance.push(
      summary.in_progress_maintenances[0].id,
    );
    expect(getHeadlineEvents(summary, dismissed, "")).toEqual(
      summary.in_progress_maintenances,
    );
  });

  it("displays scheduled maintenances only when they're within a day", () => {
    const now = nowUTC();
    const start = addHours(now, 1);
    const withinADay: ScheduledMaintenance = {
      ...getScheduledMaintenance(),
      starts_at: start.toISOString(),
    };
    const future = addDays(now, 7);
    const outsideADay: ScheduledMaintenance = {
      ...getScheduledMaintenance(),
      starts_at: future.toISOString(),
    };
    const summary = {
      ...getEmptyStatusSummary(),
      ongoing_incidents: [],
      in_progress_maintenances: [],
      scheduled_maintenances: [withinADay, outsideADay],
    };
    expect(getHeadlineEvents(summary, getEmptyDismissedEvents(), "")).toEqual([
      withinADay,
    ]);
  });
});
