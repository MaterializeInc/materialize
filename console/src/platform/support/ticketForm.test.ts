// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ATTACHMENT_LIMITS,
  attachmentError,
  buildCreateTicketRequest,
  buildTicketTargets,
  defaultTicketTarget,
  isValidFileName,
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

describe("attachmentError", () => {
  const file = (name: string, megabytes = 0.01) => ({
    name,
    size: Math.round(megabytes * 1024 * 1024),
  });

  it("allows screenshots, logs and PDFs within the limits", () => {
    expect(
      attachmentError([file("error.TXT"), file("screen.png", 4.9)]),
    ).toBeUndefined();
    expect(attachmentError([])).toBeUndefined();
  });

  it("explains what can't be attached", () => {
    expect(attachmentError([file("debug.zip")])).toMatch(
      "debug.zip can't be attached",
    );
    expect(attachmentError([file("README")])).toMatch("can't be attached");
    // Recordings go through the Messenger.
    expect(attachmentError([file("recording.mp4")])).toMatch(
      "recording.mp4 can't be attached",
    );
    expect(attachmentError([file("screen.png", 5.5)])).toBe(
      "screen.png is larger than 5 MB.",
    );
    expect(attachmentError([file("empty.txt", 0)])).toBe("empty.txt is empty.");
    expect(
      attachmentError([file("a.png", 2), file("b.png", 2), file("c.png", 2)]),
    ).toBe("Files can be up to 5 MB in total.");
    expect(
      attachmentError(Array.from({ length: 11 }, () => file("a.txt"))),
    ).toBe("Attach up to 10 files.");
  });

  it("doesn't show hidden characters from a name it rejects", () => {
    expect(attachmentError([file("invoice\u202Efdp.txt")])).toMatch(
      /^invoicefdp\.txt can't be attached: file names need a name before the extension/,
    );
    expect(attachmentError([file(".txt")])).toMatch(/^\.txt can't be attached/);
  });

  it("lists the limits for the tooltip", () => {
    expect(ATTACHMENT_LIMITS).toBe(
      "Up to 10 files, 5 MB in total: .gif, .jpeg, .jpg, .pdf, .png, .txt.",
    );
  });
});

// The same cases as the global API's file name check.
describe("isValidFileName", () => {
  it.each([
    "error.txt",
    "error log.TXT",
    "a.b.png",
    "résumé.pdf",
    "日本語.png",
    " padded.txt ",
    `${"a".repeat(251)}.txt`,
  ])("accepts %j", (name) => {
    expect(isValidFileName(name)).toBe(true);
  });

  it.each([
    ".txt",
    "..txt",
    ". .png",
    "txt",
    "a/b.txt",
    "a\\b.txt",
    "a\nb.txt",
    // Shows as "invoicetxt.pdf" but is a .txt file.
    "invoice\u202Efdp.txt",
    "zero\u200Bwidth.png",
    "\uFEFF.png",
    // Trimmed away by JavaScript, but not by the global API.
    "name.png\uFEFF",
    "tag\u{E0041}.png",
    `${"a".repeat(252)}.txt`,
  ])("rejects %j", (name) => {
    expect(isValidFileName(name)).toBe(false);
  });
});
