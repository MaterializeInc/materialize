// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Intercom ticket types the console form creates, and the attributes each
 * one requires. Pulled from `GET /ticket_types` on 2026-10-30.
 *
 * Staging and production share one Intercom workspace, so these IDs are the
 * same everywhere and anything created with them is a real ticket.
 */
export const TICKET_TYPES = {
  /** Requires Priority (Low, Medium, High). */
  supportRequest: "113560",
  /**
   * Requires Company Name, Materialize Platform and Priority. Used when the
   * Messenger doesn't know who the user is.
   */
  supportRequestVisitor: "2644579",
  /**
   * The "Report Production Outage" option in the Messenger. No extra
   * attributes. Creating one pages on-call through incident.io.
   * TODO(CNS-201): confirm with support this is the right type, not
   * "Production Outage" (2966624).
   */
  productionOutage: "2632776",
} as const;

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

/** Intercom's Priority options for the non-urgent severities. */
const PRIORITY_BY_SEVERITY: Record<
  Exclude<TicketSeverity, "urgent">,
  "High" | "Medium" | "Low"
> = {
  high: "High",
  normal: "Medium",
  low: "Low",
};

export type TicketEnvironmentType = "production" | "staging" | "development";

export const ENVIRONMENT_OPTIONS: {
  value: TicketEnvironmentType;
  label: string;
}[] = [
  { value: "production", label: "Production" },
  { value: "staging", label: "Staging" },
  { value: "development", label: "Development" },
];

/** Options of the Visitor type's "Materialize Platform" attribute. */
export const PLATFORM_OPTIONS = [
  "Materialize Cloud",
  "Materialize Emulator",
  "Self-Managed Materialize",
  "I don't have a Materialize instance",
  "I don't know",
] as const;

export type TicketPlatform = (typeof PLATFORM_OPTIONS)[number];

export type TicketFormValues = {
  title: string;
  severity: TicketSeverity;
  environmentType: TicketEnvironmentType;
  description: string;
  /** Only asked for when the user isn't identified. */
  email: string;
  name: string;
  companyName: string;
  platform: TicketPlatform;
  /** Attach the org, region and version to the ticket. */
  includeDetails: boolean;
};

/** What the console already knows, attached so nobody retypes it. */
export type TicketContext = {
  organizationId?: string;
  region?: string;
  environmentVersion?: string;
};

export type SupportTicketPayload = {
  ticketTypeId: string;
  title: string;
  /** The user's description followed by the console context. */
  description: string;
  /** Ticket type attributes, keyed by their Intercom names. */
  attributes: Record<string, string>;
  /**
   * Who the ticket is for. Omitted for signed-in users, whose identity comes
   * from their login rather than the form.
   */
  contact?: { email: string; name?: string };
  severity: TicketSeverity;
};

const contextLines = (
  values: Pick<TicketFormValues, "environmentType">,
  context: TicketContext,
) =>
  [
    `Environment: ${values.environmentType}`,
    context.organizationId && `Organization ID: ${context.organizationId}`,
    context.region && `Region: ${context.region}`,
    context.environmentVersion && `Version: ${context.environmentVersion}`,
    "Submitted from the Materialize Console",
  ].filter((line): line is string => Boolean(line));

export const buildTicketPayload = (
  values: TicketFormValues,
  { isAnonymous, ...context }: TicketContext & { isAnonymous: boolean },
): SupportTicketPayload => {
  const description = [
    values.description.trim(),
    "---",
    ...contextLines(values, context),
  ].join("\n");
  const contact = isAnonymous
    ? {
        email: values.email.trim(),
        ...(values.name.trim() ? { name: values.name.trim() } : {}),
      }
    : undefined;
  const base = {
    title: values.title.trim(),
    description,
    severity: values.severity,
    ...(contact ? { contact } : {}),
  };

  if (values.severity === "urgent") {
    return {
      ...base,
      ticketTypeId: TICKET_TYPES.productionOutage,
      attributes: {},
    };
  }

  const priority = PRIORITY_BY_SEVERITY[values.severity];
  if (isAnonymous) {
    return {
      ...base,
      ticketTypeId: TICKET_TYPES.supportRequestVisitor,
      attributes: {
        Priority: priority,
        "Company Name": values.companyName.trim(),
        "Materialize Platform": values.platform,
      },
    };
  }
  return {
    ...base,
    ticketTypeId: TICKET_TYPES.supportRequest,
    attributes: { Priority: priority },
  };
};
