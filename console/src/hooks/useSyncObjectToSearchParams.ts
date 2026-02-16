// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useEffect } from "react";
import { useLocation, useSearchParams } from "react-router-dom";

/**
 * Encodes an object as a URLSearchParams object.
 *
 * We will only encode booleans, numbers, and strings as search parameters and anything else
 * in the object will be ignored. If a value is an array, we will encode each element in the array keyed
 * by the array's key.
 */
export function encodeObjectAsSearchParams(object: Record<string, any>) {
  const searchParams = new URLSearchParams();

  function helper(curNode: unknown, curPath: string[]) {
    if (
      typeof curNode === "boolean" ||
      typeof curNode === "number" ||
      typeof curNode === "string"
    ) {
      searchParams.append(curPath.join("."), `${curNode}`);
      return;
    }

    if (Array.isArray(curNode)) {
      /**
       * For elements in an array, we want to key by the array's key rather than each element's index.
       */
      for (const value of curNode) {
        helper(value, curPath);
      }
      return;
    }

    if (typeof curNode === "object" && curNode !== null) {
      for (const curObjectKey in curNode) {
        curPath.push(curObjectKey);
        helper(curNode[curObjectKey as keyof typeof curNode], curPath);
        curPath.pop();
      }
    }
  }

  const path: string[] = [];

  helper(object, path);
  return searchParams;
}
/**
 *
 * Syncs any object to the current URL's search params.
 *
 * @param object - The object to sync to the current URL's search params. This object must be a stable and immutable.
 * @param pathPrefix - (Optional) if set, the object will only be synced to the URL when the location pathname starts with the given prefix.
 *
 * @example
 * ```
 * object = {
 *    value1: "foo",
 *    "value2[]": ["bar", "baz"],
 *    value3: {
 *      value4: "qux"
 *    }
 * }
 * ```
 * becomes `?value1=foo&value2[]=bar&value2[]=baz&value3.value4=qux`
 *
 */
export const useSyncObjectToSearchParams = (
  object: Record<string, any>,
  pathPrefix: string | undefined = undefined,
) => {
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    if (pathPrefix && !pathname.startsWith(pathPrefix)) {
      return;
    }
    const newSearchParams = encodeObjectAsSearchParams(object);
    if (newSearchParams.toString() !== searchParams.toString()) {
      setSearchParams(newSearchParams, { replace: true });
    }
  }, [searchParams, setSearchParams, object, pathname, pathPrefix]);
};
