// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render } from "@testing-library/react";
import React from "react";
import { BrowserRouter } from "react-router-dom";
import { z } from "zod";

import StripSearchIfInvalidZodSchema from "./StripSearchIfInvalidZodSchema";

describe("StripSearchIfInvalidZodSchema", () => {
  beforeEach(() => {
    history.pushState(undefined, "", "/");
  });

  it("should call children with validated data when search params matches the schema", () => {
    const searchParamString = "?param1=value1&param2=value2";

    const mockRenderProp = vi.fn(() => null);
    history.pushState(undefined, "", `/${searchParamString}`);

    render(
      <BrowserRouter>
        <StripSearchIfInvalidZodSchema
          schema={z.object({
            param1: z.string(),
            param2: z.string(),
          })}
          mapSearchParamsToSchema={(searchParams: URLSearchParams) => ({
            param1: searchParams.get("param1"),
            param2: searchParams.get("param2"),
          })}
        >
          {mockRenderProp}
        </StripSearchIfInvalidZodSchema>
      </BrowserRouter>,
    );

    expect(mockRenderProp).toHaveBeenCalledWith({
      param1: "value1",
      param2: "value2",
    });

    expect(window.location.search).toBe(searchParamString);
  });

  it("should redirect and clear the current search parameters when they do not match the schema", () => {
    const searchParamString = "?param1=notANumber";
    history.pushState(undefined, "", `/${searchParamString}`);
    expect(window.location.search).toBe(searchParamString);

    const mockRenderProp = vi.fn(() => null);

    render(
      <BrowserRouter>
        <StripSearchIfInvalidZodSchema
          schema={z.object({
            param1: z.number(),
          })}
          mapSearchParamsToSchema={(searchParams: URLSearchParams) => ({
            param1: searchParams.get("param1"),
          })}
        >
          {mockRenderProp}
        </StripSearchIfInvalidZodSchema>
      </BrowserRouter>,
    );

    expect(mockRenderProp).not.toBeCalled();

    expect(window.location.search).not.toBe(searchParamString);
  });
});
