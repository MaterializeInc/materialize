// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { downloadFile } from "./downloadFile";

describe("downloadFile", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("successfully downloads a file", () => {
    const anchor = document.createElement("a");
    const clickEventSpy = vi.spyOn(anchor, "click");
    const removeEventSpy = vi.spyOn(anchor, "remove");
    const createElementSpy = vi
      .spyOn(document, "createElement")
      .mockReturnValue(anchor);
    const appendChildSpy = vi.spyOn(document.body, "appendChild");

    document.body.innerHTML = "";
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:an_object_url");
    downloadFile("filename.csv", "testing");
    expect(createElementSpy).toHaveBeenCalled();
    expect(anchor.href).toEqual("blob:an_object_url");
    expect(anchor.download).toEqual("filename.csv");
    expect(appendChildSpy).toHaveBeenCalledWith(anchor);
    expect(clickEventSpy).toHaveBeenCalled();
    expect(removeEventSpy).toHaveBeenCalled();
  });

  it("successfully cleans up", () => {
    document.body.innerHTML = "";
    downloadFile("filename.csv", "testing");
    expect(document.body.innerHTML).toEqual("");
  });
});
