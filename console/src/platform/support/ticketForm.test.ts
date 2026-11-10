// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildCreateTicketRequest,
  buildTicketTargets,
  defaultTicketTarget,
  isUnchangedTemplate,
  TICKET_TEMPLATE,
} from "./ticketForm";

const values = {
  title: "  Sink is lagging ",
  severity: "normal" as const,
  environmentType: "production" as const,
  description: " Kafka sink is 10 minutes behind. ",
};

const signedIn = {
  organizationId: "org-1",
  organizationName: "Production",
  region: "aws/us-east-1",
  environmentVersion: "v26.48.0",
};

describe("buildCreateTicketRequest", () => {
  it("trims the fields and attaches the organization, region and version", () => {
    const [target] = buildTicketTargets([], signedIn);
    expect(buildCreateTicketRequest(values, target)).toEqual({
      title: "Sink is lagging",
      description: "Kafka sink is 10 minutes behind.",
      severity: "normal",
      environmentType: "production",
      organizationId: "org-1",
      region: "aws/us-east-1",
      environmentVersion: "v26.48.0",
    });
  });

  it("leaves out context the console doesn't have", () => {
    const request = buildCreateTicketRequest(values, {
      key: "org-2 ",
      organizationId: "org-2",
      organizationName: "Sandbox",
    });
    expect(request.organizationId).toBe("org-2");
    expect(request).not.toHaveProperty("region");
    expect(request).not.toHaveProperty("environmentVersion");
  });
});

describe("buildTicketTargets", () => {
  it("offers each organization's active regions", () => {
    const targets = buildTicketTargets(
      [
        {
          id: "org-1",
          name: "Production",
          regions: [
            { id: "aws/eu-west-1", environmentVersion: "v26.47.0" },
            { id: "aws/us-east-1", environmentVersion: "v26.47.0" },
          ],
        },
        { id: "org-2", name: "Self-managed", regions: [] },
      ],
      signedIn,
    );
    expect(
      targets.map(({ organizationName, region, environmentVersion }) => [
        organizationName,
        region,
        environmentVersion,
      ]),
    ).toEqual([
      ["Production", "aws/eu-west-1", "v26.47.0"],
      // The console's own reading of the current region wins.
      ["Production", "aws/us-east-1", "v26.48.0"],
      // An organization without an environment is still a choice.
      ["Self-managed", undefined, undefined],
    ]);
    expect(defaultTicketTarget(targets, signedIn)?.region).toBe(
      "aws/us-east-1",
    );
  });

  it("adds the signed-in organization and region when the API doesn't list them", () => {
    // Older APIs return no regions, or can't list the organizations.
    const targets = buildTicketTargets(
      [{ id: "org-2", name: "Sandbox" }],
      signedIn,
    );
    expect(
      targets.map(({ organizationId, region }) => [organizationId, region]),
    ).toEqual([
      ["org-1", "aws/us-east-1"],
      ["org-2", undefined],
    ]);
  });
});

describe("isUnchangedTemplate", () => {
  it("treats the untouched template, or only whitespace changes, as empty", () => {
    expect(isUnchangedTemplate(TICKET_TEMPLATE, TICKET_TEMPLATE)).toBe(true);
    expect(
      isUnchangedTemplate(`\n${TICKET_TEMPLATE}\n\n `, TICKET_TEMPLATE),
    ).toBe(true);
    expect(
      isUnchangedTemplate(
        TICKET_TEMPLATE.replace("What's affected:", "What's affected: a sink"),
        TICKET_TEMPLATE,
      ),
    ).toBe(false);
  });
});
