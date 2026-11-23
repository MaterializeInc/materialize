// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

/** `value`, or while it's undefined, the last value it had in a commit. */
export function useLastDefined<T>(value: T | undefined) {
  const last = React.useRef(value);
  React.useEffect(() => {
    if (value !== undefined) last.current = value;
  });
  return value ?? last.current;
}
