// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface StatusSummary {
  page_title: string;
  page_url: string;
  ongoing_incidents: Incident[];
  in_progress_maintenances: InProgressMaintenance[];
  scheduled_maintenances: ScheduledMaintenance[];
}

interface Event {
  id: string;
  name: string;
  url: string;
  last_update_at: string;
  last_update_message: string;
  affected_components: AffectedComponent[];
}

export interface AffectedComponent {
  id: string;
  name: string;
  group_name?: string;
}

export interface Incident extends Event {
  current_worst_impact:
    | "partial_outage"
    | "degraded_performance"
    | "full_outage";
  status: "identified" | "investigating" | "monitoring";
}

export interface InProgressMaintenance extends Event {
  status: "maintenance_in_progress";
  started_at: string;
  scheduled_end_at: string;
}

export interface ScheduledMaintenance extends Event {
  status: "maintenance_scheduled";
  starts_at: string;
  ends_at: string;
}

export type EventTypes =
  | Incident
  | InProgressMaintenance
  | ScheduledMaintenance;

export function isIncident(event: EventTypes): event is Incident {
  return (
    event.status !== "maintenance_in_progress" &&
    event.status !== "maintenance_scheduled"
  );
}

export function isInProgressMaintenance(
  event: EventTypes,
): event is InProgressMaintenance {
  return event.status === "maintenance_in_progress";
}

export function isScheduledMaintenance(
  event: EventTypes,
): event is ScheduledMaintenance {
  return event.status === "maintenance_scheduled";
}
