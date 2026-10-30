// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildTicketPayload,
  TICKET_TYPES,
  TicketFormValues,
} from "./ticketForm";

const values = (
  overrides: Partial<TicketFormValues> = {},
): TicketFormValues => ({
  title: "  Sink is lagging ",
  severity: "normal",
  environmentType: "production",
  description: " Kafka sink is 10 minutes behind. ",
  email: "",
  name: "",
  companyName: "",
  platform: "Materialize Cloud",
  includeDetails: true,
  ...overrides,
});

const context = {
  organizationId: "org-1",
  region: "aws/us-east-1",
  environmentVersion: "v0.150.0",
};

describe("buildTicketPayload", () => {
  it.each([
    ["high", "High"],
    ["normal", "Medium"],
    ["low", "Low"],
  ] as const)(
    "maps %s severity to the support request's %s priority",
    (severity, priority) => {
      const payload = buildTicketPayload(values({ severity }), {
        ...context,
        isAnonymous: false,
      });
      expect(payload).toEqual({
        ticketTypeId: TICKET_TYPES.supportRequest,
        title: "Sink is lagging",
        severity,
        attributes: { Priority: priority },
        description: [
          "Kafka sink is 10 minutes behind.",
          "---",
          "Environment: production",
          "Organization ID: org-1",
          "Region: aws/us-east-1",
          "Version: v0.150.0",
          "Submitted from the Materialize Console",
        ].join("\n"),
      });
    },
  );

  it("sends urgent tickets as production outages", () => {
    for (const isAnonymous of [false, true]) {
      const payload = buildTicketPayload(
        values({ severity: "urgent", email: "ada@example.com" }),
        { isAnonymous },
      );
      expect(payload.ticketTypeId).toBe(TICKET_TYPES.productionOutage);
      expect(payload.attributes).toEqual({});
    }
  });

  it("uses the visitor type with a contact for anonymous users", () => {
    const payload = buildTicketPayload(
      values({
        email: " ada@example.com ",
        name: "Ada",
        companyName: " Example Corp ",
        platform: "Self-Managed Materialize",
        severity: "low",
      }),
      { isAnonymous: true },
    );
    expect(payload).toMatchObject({
      ticketTypeId: TICKET_TYPES.supportRequestVisitor,
      contact: { email: "ada@example.com", name: "Ada" },
      attributes: {
        Priority: "Low",
        "Company Name": "Example Corp",
        "Materialize Platform": "Self-Managed Materialize",
      },
    });
  });

  it("leaves out the contact for signed-in users", () => {
    const payload = buildTicketPayload(values({ email: "ada@example.com" }), {
      isAnonymous: false,
    });
    expect(payload).not.toHaveProperty("contact");
  });

  it("leaves out context the console doesn't have", () => {
    const payload = buildTicketPayload(values(), { isAnonymous: false });
    expect(payload.description).not.toMatch(/Organization ID|Region|Version/);
  });
});
