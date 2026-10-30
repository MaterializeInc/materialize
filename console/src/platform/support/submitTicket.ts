// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SupportTicketPayload } from "./ticketForm";

export type SubmitTicketResult = {
  /** False while submission is stubbed: nothing was sent. */
  sent: boolean;
  ticketNumber?: string;
};

/**
 * Sends a ticket from the console form.
 *
 * TODO(CNS-201): not connected yet. How the form submits is still being
 * decided with design. The global API's `POST /api/organization/support/tickets`
 * (`createSupportTicket`) is one option. Before connecting anything, agree a
 * test plan with support: staging and production share one Intercom
 * workspace, and an outage ticket pages on-call.
 */
export const submitTicket = async (
  _payload: SupportTicketPayload,
): Promise<SubmitTicketResult> => ({ sent: false });
