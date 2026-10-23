// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { htmlToText } from "./supportTicketUtils";

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
