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

/** Links we keep clickable. Anything else, e.g. `javascript:`, is dropped. */
const SAFE_HREF = /^(https?:|mailto:)/i;

/** A run of message text, or a link. */
export type MessagePart = { text: string; href?: string };

/**
 * Converts Intercom's HTML message bodies to text and links, keeping
 * paragraph and line breaks. Parsing with `DOMParser` doesn't run scripts or
 * load images, and rendering the parts as text and links means no other
 * markup reaches the page.
 */
export function htmlToParts(html: string): MessagePart[] {
  const doc = new DOMParser().parseFromString(html, "text/html");
  doc.querySelectorAll("br").forEach((br) => br.replaceWith("\n"));
  doc.querySelectorAll(BLOCK_ELEMENTS).forEach((el) => el.append("\n\n"));

  const parts: MessagePart[] = [];
  const push = (text: string, href?: string) => {
    if (!text) return;
    const last = parts[parts.length - 1];
    if (last && !last.href && !href) {
      last.text += text;
    } else {
      parts.push({ text, href });
    }
  };
  const walk = (node: Node) => {
    if (node.nodeType === Node.TEXT_NODE) {
      push(node.textContent ?? "");
      return;
    }
    if (node.nodeName === "A") {
      const href = (node as Element).getAttribute("href")?.trim() ?? "";
      const text = node.textContent ?? "";
      if (SAFE_HREF.test(href)) {
        push(text.trim() ? text : href, href);
      } else {
        push(text);
      }
      return;
    }
    node.childNodes.forEach(walk);
  };
  walk(doc.body);

  for (const part of parts) {
    if (!part.href) part.text = part.text.replace(/\n{3,}/g, "\n\n");
  }
  const first = parts[0];
  if (first && !first.href) first.text = first.text.trimStart();
  const last = parts[parts.length - 1];
  if (last && !last.href) last.text = last.text.trimEnd();
  return parts.filter((part) => part.text);
}

/** The text of a message body, without links. */
export function htmlToText(html: string): string {
  return htmlToParts(html)
    .map((part) => part.text)
    .join("");
}

/** e.g. "2 hours ago". */
export function formatRelativeTime(isoDate: string): string {
  return formatDistanceToNowStrict(new Date(isoDate), { addSuffix: true });
}
