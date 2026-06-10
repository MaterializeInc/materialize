// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtom } from "jotai";
import React from "react";
import { useInView } from "react-intersection-observer";
import useResizeObserver from "use-resize-observer";

export const usePageHeadingRef = () => {
  const [, setValue] = useAtom(isPageHeadingVisible);
  const headerHeight = usePageHeaderHeight();
  const { ref, inView } = useInView({ rootMargin: `-${headerHeight ?? 0}px` });

  React.useEffect(() => {
    setValue(inView);
  }, [inView, setValue]);

  return ref;
};

export const useTrackPageHeaderHeight = () => {
  const headerRef = React.useRef<HTMLElement | null>(null);
  const [, setHeight] = useAtom(pageHeaderHeight);

  useResizeObserver<HTMLElement>({
    ref: headerRef,
    onResize: ({ height }) => {
      if (height) {
        const offsetTop = headerRef.current?.offsetTop ?? 0;
        // include offsetTop to account for the top bar on mobile layout
        setHeight(height + offsetTop);
      }
    },
  });
  return headerRef;
};

export const usePageHeaderHeight = () => {
  const [value] = useAtom(pageHeaderHeight);
  return value;
};

export const usePageHeadingInView = () => {
  const [value] = useAtom(isPageHeadingVisible);
  return value;
};

export const pageHeaderHeight = atom<number | null>(null);

export const isPageHeadingVisible = atom<boolean>(true);
