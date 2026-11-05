// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React, { useLayoutEffect, useState } from "react";

/** Tracks an element's border-box width, rounded to whole pixels. */
export function useElementWidth(ref: React.RefObject<HTMLElement>) {
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    setWidth(Math.round(element.getBoundingClientRect().width));
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.round(entry.borderBoxSize[0]?.inlineSize ?? 0));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}
