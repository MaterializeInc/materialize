// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { htmlToParts, htmlToText } from "./supportTicketUtils";

describe("htmlToText", () => {
  it("keeps paragraphs and line breaks", () => {
    expect(htmlToText("<p>Hi there,</p><p>Line one<br>Line two</p>")).toBe(
      "Hi there,\n\nLine one\nLine two",
    );
  });

  it("drops markup and scripts' effects", () => {
    expect(
      htmlToText(
        '<p onclick="x()">Click <b>me</b></p><img src=x onerror=alert(1)>',
      ),
    ).toBe("Click me");
  });
});

describe("htmlToParts", () => {
  it("keeps links", () => {
    expect(
      htmlToParts(
        '<p>See <a href="https://materialize.com/docs/">the docs</a>.</p>',
      ),
    ).toEqual([
      { text: "See " },
      { text: "the docs", href: "https://materialize.com/docs/" },
      { text: "." },
    ]);
  });

  it("drops unsafe links but keeps their text", () => {
    expect(htmlToParts('<a href="javascript:alert(1)">Click</a> here')).toEqual(
      [{ text: "Click here" }],
    );
  });

  it("shows the URL of a link without text", () => {
    expect(
      htmlToParts('<a href="mailto:support@materialize.com"></a>'),
    ).toEqual([
      {
        text: "mailto:support@materialize.com",
        href: "mailto:support@materialize.com",
      },
    ]);
  });
});
