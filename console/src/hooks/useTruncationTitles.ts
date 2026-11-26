// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

const isTruncated = (element: HTMLElement) => {
  const style = getComputedStyle(element);
  return (
    style.textOverflow === "ellipsis" &&
    (element.scrollWidth > element.clientWidth ||
      // NOTE: Inter's glyphs overflow a tight line box by a few pixels even
      // when every line fits. A clamped line overflows it by a whole line.
      element.scrollHeight - element.clientHeight >
        (parseFloat(style.lineHeight) || 0) / 2)
  );
};

/**
 * While the pointer is over text cut off by an ellipsis, gives it a `title`, so
 * the browser shows the full text.
 */
export const useTruncationTitles = () => {
  React.useEffect(() => {
    const onPointerOver = (event: PointerEvent) => {
      let element = event.target instanceof HTMLElement ? event.target : null;
      while (element && !isTruncated(element)) {
        element = element.parentElement;
      }
      if (!element || element.closest("[title]")) return;
      const titled = element;
      const setTitle = () => {
        titled.title = titled.textContent ?? "";
      };
      setTitle();
      // NOTE: Sentry copies an element's `title` into click breadcrumbs and
      // interaction span names. Given this name, it uses the name alone.
      titled.dataset.sentryElement = titled.localName;
      // NOTE: A live table reuses a row's elements for another row's data, so
      // the text can change under the pointer.
      const observer = new MutationObserver(setTitle);
      observer.observe(titled, {
        characterData: true,
        childList: true,
        subtree: true,
      });
      titled.addEventListener(
        "pointerleave",
        () => {
          observer.disconnect();
          titled.removeAttribute("title");
          delete titled.dataset.sentryElement;
        },
        { once: true },
      );
    };
    document.addEventListener("pointerover", onPointerOver);
    return () => document.removeEventListener("pointerover", onPointerOver);
  }, []);
};
