// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { formatDistanceToNowStrict } from "date-fns";

const BLOCK_ELEMENTS = "p, div, li, h1, h2, h3, h4, h5, h6, pre, blockquote";

/**
 * Converts Intercom's HTML message bodies to plain text, keeping paragraph
 * and line breaks. Parsing with `DOMParser` doesn't run scripts or load
 * images, and rendering the result as text means no markup reaches the page.
 */
export function htmlToText(html: string): string {
  const doc = new DOMParser().parseFromString(html, "text/html");
  doc.querySelectorAll("br").forEach((br) => br.replaceWith("\n"));
  doc.querySelectorAll(BLOCK_ELEMENTS).forEach((el) => el.append("\n\n"));
  return (doc.body.textContent ?? "").replace(/\n{3,}/g, "\n\n").trim();
}

/** e.g. "2 hours ago". */
export function formatRelativeTime(isoDate: string): string {
  return formatDistanceToNowStrict(new Date(isoDate), { addSuffix: true });
}
