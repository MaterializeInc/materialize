// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook, waitFor } from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";

import { useQueryStringState } from "./useQueryString";

describe("useQueryStringState", () => {
  beforeEach(() => {
    history.pushState(undefined, "", "/");
  });

  it("should not set a query string key when the state is falsey", async () => {
    const {
      result: {
        current: [val, _setVal],
      },
    } = renderHook(() => useQueryStringState("key"), {
      wrapper: BrowserRouter,
    });
    expect(val).toBe(undefined);
    await waitFor(() => expect(location.search).toBe(""));
  });

  it("updates the state value with the current query string value", async () => {
    history.pushState(undefined, "", "/?key=startingValue");
    await waitFor(() => expect(location.search).toBe("?key=startingValue"));
    const { result } = renderHook(() => useQueryStringState("key"), {
      wrapper: BrowserRouter,
    });
    const [val] = result.current;
    expect(val).toBe("startingValue");
  });

  it("updates the query string when setValue is called", async () => {
    const { result } = renderHook(() => useQueryStringState("key"), {
      wrapper: BrowserRouter,
    });
    let [val, setVal] = result.current;
    act(() => setVal("value"));
    [val, setVal] = result.current;
    expect(val).toBe("value");
    await waitFor(() => expect(location.search).toBe("?key=value"));
  });

  it("removes the query string key when the value is falsey", async () => {
    const { result } = renderHook(() => useQueryStringState("key"), {
      wrapper: BrowserRouter,
    });
    let [val, setVal] = result.current;
    act(() => setVal(""));
    [val, setVal] = result.current;
    expect(val).toBe("");
    await waitFor(() => expect(location.search).toBe(""));
  });
});
