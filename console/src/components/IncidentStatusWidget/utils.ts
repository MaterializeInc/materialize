// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { isBefore, parseISO, subDays } from "date-fns";

import { EventTypes, StatusSummary } from "~/api/incident-io/types";

export type LastDismissedEvents = {
  ongoing_incident: string[];
  // We opt to not consolidate in_progress and scheduled event IDs into a
  // single collection since we want a dismissed scheduled maintenance to
  // display a toast when it starts.
  in_progress_maintenance: string[];
  scheduled_maintenance: string[];
};

export function isInRegion(
  event: EventTypes,
  regionId: string | null,
): boolean {
  const affectedRegions = event.affected_components.filter(
    (c) => c.group_name === "Cloud regions",
  );
  // If we have a region set and region components are affected, assume this is to be filtered.
  if (regionId && affectedRegions.length > 0) {
    return !!affectedRegions.find(
      (r) => r.name.toLowerCase() === regionId.toLowerCase(),
    );
  }
  // Otherwise, do not attempt to filter.
  return true;
}

/**
 * Get the events to display to the user based on perceived priority.
 */
export function getHeadlineEvents(
  summary: StatusSummary,
  dismissed: LastDismissedEvents,
  currentRegionId: string | null,
): EventTypes[] {
  if (summary.ongoing_incidents.length > 0) {
    const incidents = summary.ongoing_incidents.filter(
      (incident) => !dismissed.ongoing_incident.includes(incident.id),
    );
    if (incidents.length > 0) {
      return incidents;
    }
  }
  if (summary.in_progress_maintenances.length > 0) {
    const inProgress = summary.in_progress_maintenances.filter(
      (maint) =>
        !dismissed.in_progress_maintenance.includes(maint.id) &&
        isInRegion(maint, currentRegionId),
    );
    if (inProgress.length > 0) {
      return inProgress;
    }
  }
  if (summary.scheduled_maintenances.length > 0) {
    const now = new Date();
    const upcoming = summary.scheduled_maintenances.filter((maint) => {
      const startDate = parseISO(maint.starts_at);
      return (
        isBefore(subDays(startDate, 1), now) &&
        !dismissed.scheduled_maintenance.includes(maint.id) &&
        isInRegion(maint, currentRegionId)
      );
    });
    return upcoming;
  }
  return [];
}
