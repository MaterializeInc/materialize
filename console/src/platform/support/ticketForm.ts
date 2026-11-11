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
  SupportOrganization,
  SupportTicketEnvironmentType,
} from "~/api/cloudGlobalApi";

/**
 * Severities shown in the form. The global API opens urgent tickets as
 * Production Outage tickets, which page on-call for organizations with an
 * SLA, and the rest as "Support Request" tickets with a matching Priority.
 */
export type TicketSeverity = CreateSupportTicketRequest["severity"];

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

/** The default form's severities. Outages have their own form. */
export const TICKET_SEVERITY_OPTIONS = SEVERITY_OPTIONS.filter(
  (option) => option.value !== "urgent",
);

export const ENVIRONMENT_OPTIONS: {
  value: SupportTicketEnvironmentType;
  label: string;
}[] = [
  { value: "production", label: "Production" },
  { value: "staging", label: "Staging" },
  { value: "development", label: "Development" },
];

/**
 * Files a ticket can carry, matching the global API's limits: Intercom's
 * default file types for customer attachments, and up to 10 files (Intercom's
 * limit per reply), 10 MB each and 20 MB in total.
 */
export const ATTACHMENT_EXTENSIONS = [
  "gif",
  "jpeg",
  "jpg",
  "mov",
  "mp4",
  "pdf",
  "png",
  "txt",
  "heic",
  "oga",
  "ogg",
  "dng",
  "mp3",
];
export const MAX_ATTACHMENTS = 10;
export const MAX_ATTACHMENT_MB = 10;
export const MAX_ATTACHMENTS_TOTAL_MB = 20;
const MB = 1024 * 1024;

/** The limits, for the attachment button's tooltip. */
export const ATTACHMENT_LIMITS = `Up to ${MAX_ATTACHMENTS} files, ${MAX_ATTACHMENT_MB} MB each and ${MAX_ATTACHMENTS_TOTAL_MB} MB in total: ${ATTACHMENT_EXTENSIONS.map(
  (extension) => `.${extension}`,
).join(", ")}.`;

/** The value for the file input's `accept`. */
export const ATTACHMENT_ACCEPT = ATTACHMENT_EXTENSIONS.map(
  (extension) => `.${extension}`,
).join(",");

const extensionOf = (name: string) =>
  name.includes(".") ? name.split(".").pop()?.toLowerCase() : undefined;

/** Why these files can't be attached, or undefined if they can. */
export const attachmentError = (files: { name: string; size: number }[]) => {
  if (files.length > MAX_ATTACHMENTS) {
    return `Attach up to ${MAX_ATTACHMENTS} files.`;
  }
  const unsupported = files.find(
    ({ name }) => !ATTACHMENT_EXTENSIONS.includes(extensionOf(name) ?? ""),
  );
  if (unsupported) {
    return `${unsupported.name} can't be attached. Supported types: ${ATTACHMENT_ACCEPT.replaceAll(",", ", ")}.`;
  }
  const tooLarge = files.find(({ size }) => size > MAX_ATTACHMENT_MB * MB);
  if (tooLarge) {
    return `${tooLarge.name} is larger than ${MAX_ATTACHMENT_MB} MB.`;
  }
  const empty = files.find(({ size }) => size === 0);
  if (empty) {
    return `${empty.name} is empty.`;
  }
  const total = files.reduce((sum, { size }) => sum + size, 0);
  if (total > MAX_ATTACHMENTS_TOTAL_MB * MB) {
    return `Files can be up to ${MAX_ATTACHMENTS_TOTAL_MB} MB in total.`;
  }
  return undefined;
};

/** A file's contents, base64-encoded, as the global API takes them. */
export const readAsBase64 = (file: Blob) =>
  new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result);
      // A data URL: everything after the comma is the base64 data.
      resolve(result.slice(result.indexOf(",") + 1));
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });

/** What the description box starts with, so tickets arrive with the basics. */
export const TICKET_TEMPLATE = `What's affected:
When it started (with time zone):
Is it still happening?
What changed recently (upgrade, deploy, upstream change):
Error messages, if available:`;

export const OUTAGE_TEMPLATE = `What's down (whole environment, or specific clusters, sources, sinks, or queries):
When it started (with time zone):
What changed right before (upgrade, deploy, upstream change):
Error messages, if available:`;

const withoutBlankLines = (text: string) =>
  text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .join("\n");

/** Whether a description adds nothing to the template it started from. */
export const isUnchangedTemplate = (description: string, template: string) =>
  withoutBlankLines(description) === withoutBlankLines(template);

export type TicketFormValues = {
  title: string;
  severity: TicketSeverity;
  environmentType: SupportTicketEnvironmentType;
  description: string;
  /** The `key` of the chosen `TicketTarget`. */
  target: string;
};

/** An organization, and one of its regions, that a ticket can be about. */
export type TicketTarget = {
  key: string;
  organizationId: string;
  organizationName: string;
  region?: string;
  environmentVersion?: string;
};

/** What the console knows about the signed-in organization's environment. */
export type SignedInEnvironment = {
  organizationId?: string;
  organizationName?: string;
  region?: string;
  environmentVersion?: string;
};

const targetKey = (organizationId: string, region?: string) =>
  `${organizationId} ${region ?? ""}`;

/**
 * One choice per organization and active region, or one for an organization
 * with none. The signed-in organization is always offered, with the current
 * region and the version the console reads from it, in case the API doesn't
 * know them (older APIs return no regions at all).
 */
export const buildTicketTargets = (
  organizations: SupportOrganization[],
  signedIn: SignedInEnvironment,
): TicketTarget[] => {
  const all = [...organizations];
  if (
    signedIn.organizationId &&
    !all.some(({ id }) => id === signedIn.organizationId)
  ) {
    all.unshift({
      id: signedIn.organizationId,
      name: signedIn.organizationName ?? signedIn.organizationId,
    });
  }
  return all.flatMap((organization) => {
    const regions = (organization.regions ?? []).map((region) => ({
      id: region.id,
      environmentVersion: region.environmentVersion ?? undefined,
    }));
    if (organization.id === signedIn.organizationId && signedIn.region) {
      const current = regions.find(({ id }) => id === signedIn.region);
      if (current) {
        current.environmentVersion =
          signedIn.environmentVersion ?? current.environmentVersion;
      } else {
        regions.push({
          id: signedIn.region,
          environmentVersion: signedIn.environmentVersion,
        });
      }
    }
    const base = {
      organizationId: organization.id,
      organizationName: organization.name,
    };
    if (regions.length === 0) {
      return [{ ...base, key: targetKey(organization.id) }];
    }
    return regions.map((region) => ({
      ...base,
      key: targetKey(organization.id, region.id),
      region: region.id,
      environmentVersion: region.environmentVersion,
    }));
  });
};

/** The choice a new ticket starts on: the signed-in organization and region. */
export const defaultTicketTarget = (
  targets: TicketTarget[],
  signedIn: SignedInEnvironment,
) =>
  targets.find(
    (target) =>
      target.organizationId === signedIn.organizationId &&
      target.region === signedIn.region,
  ) ??
  targets.find((target) => target.organizationId === signedIn.organizationId) ??
  targets[0];

/**
 * The request for a ticket. The global API identifies the user from their
 * login and adds the org ID and their email to the description.
 */
export const buildCreateTicketRequest = (
  values: Omit<TicketFormValues, "target">,
  target: TicketTarget | undefined,
): CreateSupportTicketRequest => ({
  title: values.title.trim(),
  description: values.description.trim(),
  severity: values.severity,
  environmentType: values.environmentType,
  ...(target ? { organizationId: target.organizationId } : {}),
  ...(target?.region ? { region: target.region } : {}),
  ...(target?.environmentVersion
    ? { environmentVersion: target.environmentVersion }
    : {}),
});
