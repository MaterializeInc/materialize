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
import { formatBytesShort } from "~/utils/format";

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
 * Files a ticket can carry, matching the global API's limits: screenshots,
 * logs and PDFs, up to 10 files (Intercom's limit per reply) and 5 MB in
 * total. Larger files and recordings go through the Messenger or a reply to
 * the confirmation email.
 */
export const ATTACHMENT_EXTENSIONS = [
  "gif",
  "jpeg",
  "jpg",
  "pdf",
  "png",
  "txt",
];
export const MAX_ATTACHMENTS = 10;
/** The most a ticket's files can add up to, and so the most for one file. */
export const MAX_ATTACHMENTS_BYTES = 5 * 1024 * 1024;
const MAX_ATTACHMENTS_SIZE = formatBytesShort(BigInt(MAX_ATTACHMENTS_BYTES));
const DOTTED_EXTENSIONS = ATTACHMENT_EXTENSIONS.map(
  (extension) => `.${extension}`,
);

/** The limits, for the attachment button's tooltip. */
export const ATTACHMENT_LIMITS = `Up to ${MAX_ATTACHMENTS} files, ${MAX_ATTACHMENTS_SIZE} in total: ${DOTTED_EXTENSIONS.join(", ")}.`;

/** The value for the file input's `accept`. */
export const ATTACHMENT_ACCEPT = DOTTED_EXTENSIONS.join(",");

export const MAX_FILE_NAME_LENGTH = 255;

const extensionOf = (name: string) =>
  name.includes(".") ? name.split(".").pop()?.toLowerCase() : undefined;

/**
 * Control characters, and characters that don't show or that change how the
 * text around them is displayed, so a name could look like it has a
 * different extension. The global API rejects the same ones: keep this in
 * sync with `is_invisible` in cloud's `src/sync/src/support_tickets.rs`.
 */
const HIDDEN_CHARACTER_RANGES: [number, number][] = [
  [0x0000, 0x001f],
  [0x007f, 0x009f],
  [0x00ad, 0x00ad],
  [0x034f, 0x034f],
  [0x061c, 0x061c],
  [0x115f, 0x1160],
  [0x17b4, 0x17b5],
  [0x180b, 0x180f],
  [0x200b, 0x200f],
  [0x2028, 0x202e],
  [0x2060, 0x206f],
  [0x3164, 0x3164],
  [0xfe00, 0xfe0f],
  [0xfeff, 0xfeff],
  [0xffa0, 0xffa0],
  [0xfff0, 0xfffb],
  [0xe0000, 0xe0fff],
];

const isHiddenCharacter = (character: string) => {
  const code = character.codePointAt(0) ?? 0;
  return HIDDEN_CHARACTER_RANGES.some(
    ([first, last]) => code >= first && code <= last,
  );
};

/**
 * Whether the global API takes this file name: a name before the extension,
 * at most 255 characters once trimmed, and no slashes, control characters or
 * hidden characters anywhere.
 */
export const isValidFileName = (name: string) => {
  const characters = [...name];
  if (
    characters.some(
      (character) =>
        character === "/" || character === "\\" || isHiddenCharacter(character),
    )
  ) {
    return false;
  }
  const trimmed = name.trim();
  const dot = trimmed.lastIndexOf(".");
  const stem = trimmed
    .slice(0, Math.max(dot, 0))
    .replace(/^\.+|\.+$/g, "")
    .trim();
  return dot > 0 && stem !== "" && [...trimmed].length <= MAX_FILE_NAME_LENGTH;
};

/** Why these files can't be attached, or undefined if they can. */
export const attachmentError = (files: { name: string; size: number }[]) => {
  if (files.length > MAX_ATTACHMENTS) {
    return `Attach up to ${MAX_ATTACHMENTS} files.`;
  }
  const badName = files.find(({ name }) => !isValidFileName(name));
  if (badName) {
    const shown = [...badName.name]
      .filter((character) => !isHiddenCharacter(character))
      .join("")
      .trim();
    return `${shown || "A file"} can't be attached: file names need a name before the extension, at most ${MAX_FILE_NAME_LENGTH} characters, and no slashes or hidden characters.`;
  }
  const unsupported = files.find(
    ({ name }) =>
      !ATTACHMENT_EXTENSIONS.includes(extensionOf(name.trim()) ?? ""),
  );
  if (unsupported) {
    return `${unsupported.name} can't be attached. Supported types: ${DOTTED_EXTENSIONS.join(", ")}.`;
  }
  const tooLarge = files.find(({ size }) => size > MAX_ATTACHMENTS_BYTES);
  if (tooLarge) {
    return `${tooLarge.name} is larger than ${MAX_ATTACHMENTS_SIZE}.`;
  }
  const empty = files.find(({ size }) => size === 0);
  if (empty) {
    return `${empty.name} is empty.`;
  }
  const total = files.reduce((sum, { size }) => sum + size, 0);
  if (total > MAX_ATTACHMENTS_BYTES) {
    return `Files can be up to ${MAX_ATTACHMENTS_SIZE} in total.`;
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

/** Placeholder prompts for a regular support ticket. */
export const TICKET_TEMPLATE = `Include what you can:
• What's affected
• When it started (with time zone), and whether it's still happening
• What changed recently: an upgrade, deploy, or upstream change
• Any error messages`;

/** Placeholder prompts for an outage report. */
export const OUTAGE_TEMPLATE = `Include what you can:
• What's down: the whole environment, or specific clusters, sources, sinks, or queries
• When it started (with time zone)
• What changed right before: an upgrade, deploy, or upstream change
• Any error messages`;

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
