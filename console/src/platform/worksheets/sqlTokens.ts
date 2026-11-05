// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * A segment of SQL text. Only `code` segments can hold cell references,
 * parentheses or statement boundaries; comments and quoted text are opaque.
 */
export interface SqlSegment {
  kind: "code" | "comment" | "quoted";
  text: string;
}

const DOLLAR_TAG_REGEX = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/;

/**
 * Splits SQL into code, comments (`--`, nested `/* *\/`) and quoted text
 * (`'...'`, `E'...'`, `"..."`, `$tag$...$tag$`). An unterminated comment or
 * quote runs to the end of the input, as it does for the server's lexer.
 */
export function splitSql(sql: string): SqlSegment[] {
  const segments: SqlSegment[] = [];
  let code = "";
  const flushCode = () => {
    if (code) segments.push({ kind: "code", text: code });
    code = "";
  };
  let i = 0;
  while (i < sql.length) {
    const ch = sql[i];
    const next = sql[i + 1];
    let end: number | undefined;
    let kind: SqlSegment["kind"] = "quoted";
    if (ch === "-" && next === "-") {
      kind = "comment";
      const newline = sql.indexOf("\n", i);
      end = newline === -1 ? sql.length : newline;
    } else if (ch === "/" && next === "*") {
      kind = "comment";
      let depth = 0;
      let j = i;
      while (j < sql.length) {
        if (sql.startsWith("/*", j)) {
          depth++;
          j += 2;
        } else if (sql.startsWith("*/", j)) {
          depth--;
          j += 2;
          if (depth === 0) break;
        } else {
          j++;
        }
      }
      end = j;
    } else if (ch === "'" || ch === '"') {
      // A doubled quote escapes itself. `E'...'` strings also allow `\'`.
      const escapes = ch === "'" && /[eE]$/.test(code);
      let j = i + 1;
      while (j < sql.length) {
        if (escapes && sql[j] === "\\") {
          j += 2;
        } else if (sql[j] === ch) {
          if (sql[j + 1] === ch) j += 2;
          else break;
        } else {
          j++;
        }
      }
      end = Math.min(j + 1, sql.length);
    } else if (ch === "$") {
      const tag = DOLLAR_TAG_REGEX.exec(sql.slice(i));
      // `$1` is a parameter, not a quote, and the tag cannot follow an
      // identifier character (`a$b$` is one identifier).
      if (tag && !/[A-Za-z0-9_]$/.test(code)) {
        const close = sql.indexOf(tag[0], i + tag[0].length);
        end = close === -1 ? sql.length : close + tag[0].length;
      }
    }
    if (end === undefined) {
      code += ch;
      i++;
    } else {
      flushCode();
      segments.push({ kind, text: sql.slice(i, end) });
      i = end;
    }
  }
  flushCode();
  return segments;
}

/** Applies `fn` to the code segments of `sql`, leaving comments and quotes intact. */
export function mapSqlCode(sql: string, fn: (code: string) => string) {
  return splitSql(sql)
    .map((s) => (s.kind === "code" ? fn(s.text) : s.text))
    .join("");
}

/** Strips leading whitespace and comments. */
export function stripLeadingTrivia(sql: string) {
  const segments = splitSql(sql);
  let i = 0;
  while (
    i < segments.length &&
    (segments[i].kind === "comment" ||
      (segments[i].kind === "code" && !segments[i].text.trim()))
  ) {
    i++;
  }
  const rest = segments.slice(i);
  if (rest[0]?.kind === "code") {
    rest[0] = { kind: "code", text: rest[0].text.trimStart() };
  }
  return rest.map((s) => s.text).join("");
}

/**
 * Strips trailing whitespace, comments and semicolons, so the statement can
 * be wrapped in parentheses without a `--` comment swallowing the closing one.
 */
export function trimStatement(sql: string) {
  const segments = splitSql(sql);
  for (;;) {
    const last = segments.at(-1);
    if (!last) break;
    if (last.kind === "comment") {
      segments.pop();
    } else if (last.kind === "code") {
      const trimmed = last.text.replace(/[\s;]+$/, "");
      if (trimmed === last.text) break;
      if (trimmed) {
        segments[segments.length - 1] = { kind: "code", text: trimmed };
        break;
      }
      segments.pop();
    } else {
      break;
    }
  }
  return segments.map((s) => s.text).join("");
}

/**
 * Returns the index just past the parenthesis that closes the one at
 * `sql[0]`, ignoring parentheses in comments and quotes.
 */
export function matchParen(sql: string): number | undefined {
  let depth = 0;
  let offset = 0;
  for (const segment of splitSql(sql)) {
    if (segment.kind === "code") {
      for (let i = 0; i < segment.text.length; i++) {
        const ch = segment.text[i];
        if (ch === "(") depth++;
        else if (ch === ")" && --depth === 0) return offset + i + 1;
      }
    }
    offset += segment.text.length;
  }
  return undefined;
}

/** Whether `sql` has anything to run, that is, more than comments. */
export function hasCode(sql: string) {
  return splitSql(sql).some((s) => s.kind !== "comment" && s.text.trim());
}
