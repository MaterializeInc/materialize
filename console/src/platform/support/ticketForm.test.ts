// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildCreateTicketRequest } from "./ticketForm";

const values = {
  title: "  Sink is lagging ",
  severity: "normal" as const,
  environmentType: "production" as const,
  description: " Kafka sink is 10 minutes behind. ",
  includeDetails: true,
};

const context = { region: "aws/us-east-1", environmentVersion: "v26.48.0" };

describe("buildCreateTicketRequest", () => {
  it("trims the fields and attaches the region and version", () => {
    expect(buildCreateTicketRequest(values, context)).toEqual({
      title: "Sink is lagging",
      description: "Kafka sink is 10 minutes behind.",
      severity: "normal",
      environmentType: "production",
      region: "aws/us-east-1",
      environmentVersion: "v26.48.0",
    });
  });

  it("leaves out the region and version when details are off", () => {
    const request = buildCreateTicketRequest(
      { ...values, includeDetails: false },
      context,
    );
    expect(request).not.toHaveProperty("region");
    expect(request).not.toHaveProperty("environmentVersion");
  });

  it("leaves out context the console doesn't have", () => {
    const request = buildCreateTicketRequest(values, {});
    expect(request).not.toHaveProperty("region");
    expect(request).not.toHaveProperty("environmentVersion");
  });
});
