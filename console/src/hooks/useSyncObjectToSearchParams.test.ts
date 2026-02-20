// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { act } from "react-dom/test-utils";
import { BrowserRouter } from "react-router-dom";

import {
  encodeObjectAsSearchParams,
  useSyncObjectToSearchParams,
} from "./useSyncObjectToSearchParams";

describe("encodeObjectAsSearchParams", () => {
  it("should encode a flat object of primitive types to a URL search params object", () => {
    const object = {
      key1: "value1",
      key2: 1,
      key3: false,
      key4: undefined,
      key5: null,
      key6: new Date(),
      key7: BigInt(100),
    };
    const searchParams = encodeObjectAsSearchParams(object);

    expect(searchParams.toString()).toEqual(
      encodeURI("key1=value1&key2=1&key3=false"),
    );
  });

  it("should encode a nested object to a URL search params object", () => {
    const object = {
      key1: "value1",
      key2: {
        nestedKey1: "nestedValue1",
        "nestedKey2[]": ["foo", "bar"],
        nestedKey3: {
          nestedNestedKey1: "nestedNestedValue1",
        },
      },
    };

    const searchParams = encodeObjectAsSearchParams(object);

    expect(searchParams.toString()).toEqual(
      encodeURI(
        "key1=value1&key2.nestedKey1=nestedValue1&key2.nestedKey2[]=foo&key2.nestedKey2[]=bar&key2.nestedKey3.nestedNestedKey1=nestedNestedValue1",
      ),
    );
  });

  it("should copy the search parameter for array values", () => {
    const object = {
      "key1[]": ["value1", "value2"],
    };

    const searchParams = encodeObjectAsSearchParams(object);

    expect(searchParams.toString()).toEqual(
      encodeURI("key1[]=value1&key1[]=value2"),
    );
  });

  it("should not encode an array search parameter if empty", () => {
    const object = {
      "key1[]": [],
    };

    const searchParams = encodeObjectAsSearchParams(object);

    expect(searchParams.toString()).toEqual("");
  });
});

describe("useSyncObjectToSearchParams", () => {
  it("should set the query string key when the object changes", async () => {
    const input = {
      object: {
        key1: "value1",
      },
    };

    const hook = renderHook(
      () => {
        useSyncObjectToSearchParams(input.object);
      },
      {
        wrapper: BrowserRouter,
      },
    );

    await waitFor(() =>
      expect(location.search).toBe(encodeURI("?key1=value1")),
    );
    act(() => {
      input.object = { key1: "value2" };
      hook.rerender();
    });

    await waitFor(() =>
      expect(location.search).toBe(encodeURI("?key1=value2")),
    );
  });
});
