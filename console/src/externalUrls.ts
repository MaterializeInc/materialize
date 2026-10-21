// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Materialize chat link, redirects to the community Slack. Used both as a
 * "contact support" and a "join our Slack" call to action.
 */
export const SUPPORT_CHAT_URL = "https://materialize.com/s/chat";

/** Materialize support page, listing every support channel. */
export const SUPPORT_URL = "https://materialize.com/support/";

/** Materialize support email address. */
export const SUPPORT_EMAIL = "support@materialize.com";

/** Materialize public status page. */
export const STATUS_PAGE_URL = "https://status.materialize.com";

/** Intercom Help Center home. */
export const HELP_CENTER_URL = "https://help.materialize.com/en/";

/**
 * Help Center ticket portal. Intercom shows the tickets of the signed-in
 * user's company, so the URL needs no organization ID.
 */
export const HELP_CENTER_TICKETS_URL =
  "https://help.materialize.com/en/tickets-portal";

/** Urgent-issue process and response times for SLA-supported customers. */
export const SUPPORT_PLANS_URL = `${SUPPORT_URL}#urgent-issues-for-sla-supported-customers-only`;

/** Invite link to the Materialize Community Slack. */
export const COMMUNITY_SLACK_URL =
  "https://materializecommunity.slack.com/join/shared_invite/zt-4cxl88spz-HrApVNaPVGuhP2Vhm44PWA";
