// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  CreateSupportTicketRequest,
  SupportTicketEnvironmentType,
} from "~/api/cloudGlobalApi";

/**
 * Severities shown in the form. Urgent is a production outage: it goes
 * through the Messenger's "Report Production Outage" workflow, which pages
 * on-call, rather than the ticket endpoint. The rest are opened by the global
 * API as "Support Request" tickets with a matching Priority.
 */
export type TicketSeverity = "urgent" | "high" | "normal" | "low";

export const SEVERITY_OPTIONS: {
  value: TicketSeverity;
  label: string;
  description: string;
}[] = [
  {
    value: "urgent",
    label: "Urgent",
    description: "Production down or unusable, no workaround",
  },
  {
    value: "high",
    label: "High",
    description: "Key features seriously impaired",
  },
  {
    value: "normal",
    label: "Normal",
    description: "Some impact, a workaround exists",
  },
  {
    value: "low",
    label: "Low",
    description: "Little or no impact, or a question",
  },
];

export const ENVIRONMENT_OPTIONS: {
  value: SupportTicketEnvironmentType;
  label: string;
}[] = [
  { value: "production", label: "Production" },
  { value: "staging", label: "Staging" },
  { value: "development", label: "Development" },
];

export type TicketFormValues = {
  title: string;
  severity: TicketSeverity;
  environmentType: SupportTicketEnvironmentType;
  description: string;
  /** Attach the region and version. The org ID is always attached. */
  includeDetails: boolean;
};

/** What the console knows about the environment the ticket is about. */
export type TicketContext = {
  region?: string;
  environmentVersion?: string;
};

/**
 * The request for a non-urgent ticket. The global API identifies the user
 * from their login and adds the org ID and their email to the description.
 */
export const buildCreateTicketRequest = (
  values: TicketFormValues & {
    severity: Exclude<TicketSeverity, "urgent">;
  },
  context: TicketContext,
): CreateSupportTicketRequest => ({
  title: values.title.trim(),
  description: values.description.trim(),
  severity: values.severity,
  environmentType: values.environmentType,
  ...(values.includeDetails && context.region
    ? { region: context.region }
    : {}),
  ...(values.includeDetails && context.environmentVersion
    ? { environmentVersion: context.environmentVersion }
    : {}),
});
