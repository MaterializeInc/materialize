// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useRef } from "react";

/**
 * Returns a snapshot of a non-function value on mount in the form of a ref.
 * Useful when needing a snapshot of a prop value in a useEffect that only occurs on mount and unmount.
 * The caveat is because this is in a custom hook, you still have to put the ref in the dependency array.
 *
 * TODO: Store a .structuredClone of an object in the ref to get a pure snapshot. Ignoring for now since Jest
 * doesn't support .structuredClone
 */
export const useValueOnMount = <T>(value: T) => {
  return useRef(value);
};
