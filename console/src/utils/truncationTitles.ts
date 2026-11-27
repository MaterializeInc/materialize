// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const isTruncated = (element: HTMLElement, style: CSSStyleDeclaration) =>
  style.textOverflow === "ellipsis" &&
  (element.scrollWidth > element.clientWidth ||
    // NOTE: Inter's glyphs overflow a tight line box by a few pixels even
    // when every line fits. A clamped line overflows it by a whole line.
    element.scrollHeight - element.clientHeight >
      (parseFloat(style.lineHeight) || 0) / 2);

const findTruncated = (target: EventTarget | null) => {
  let element = target instanceof HTMLElement ? target : null;
  while (element) {
    const style = getComputedStyle(element);
    if (isTruncated(element, style)) return element;
    // `text-overflow` cuts off the lines of the element it is set on. Only an
    // inline element sits in a line of an ancestor, so the walk ends at the
    // first element that is not inline.
    if (!style.display.startsWith("inline")) return null;
    element = element.parentElement;
  }
  return null;
};

const onPointerOver = (event: PointerEvent) => {
  const element = findTruncated(event.target);
  if (!element || element.closest("[title], [data-no-truncation-title]")) {
    return;
  }
  element.title = element.textContent ?? "";
  // NOTE: Sentry copies an element's `title` into click breadcrumbs and
  // interaction span names. Given this name, it uses the name alone.
  element.dataset.sentryElement = element.localName;
  element.addEventListener(
    "pointerleave",
    () => {
      element.removeAttribute("title");
      delete element.dataset.sentryElement;
    },
    { once: true },
  );
};

/**
 * While the pointer is over text cut off by an ellipsis, gives it a `title`, so
 * the browser shows the full text. Returns a function that stops doing so.
 *
 * Text inside an element with `data-no-truncation-title` is left alone. Set it
 * where a `Tooltip` already shows the full text.
 */
export const registerTruncationTitles = () => {
  document.addEventListener("pointerover", onPointerOver);
  return () => document.removeEventListener("pointerover", onPointerOver);
};
