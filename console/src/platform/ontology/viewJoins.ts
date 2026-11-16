// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Reads the joins out of a view's definition. The input is the `create_sql`
 * the catalog stores, which Materialize prints from the resolved AST: every
 * relation appears as `[u4 AS "db"."schema"."name"]`, carrying its catalog id,
 * and every identifier is quoted, so bare words are keywords. That regular
 * form is what lets a token scan stand in for a parser. Handwritten SQL does
 * not have it and is not a supported input.
 *
 * The scan errs toward missing a join over reporting a wrong one: an alias
 * bound to two relations anywhere in the statement, or bound to anything that
 * is not a catalog relation (a subquery, CTE or table function), is never
 * resolved.
 */

export interface ColumnRef {
  objectId: string;
  column: string;
}

/** Two columns a view equates, as in `a.x = b.y`, in no particular order. */
export interface JoinColumnPair {
  left: ColumnRef;
  right: ColumnRef;
}

/**
 * A `JOIN ... USING (columns)`. The right side is known, the left side is any
 * relation joined before it in the same FROM clause, which only the relations'
 * columns can settle.
 */
export interface UsingJoin {
  right: string;
  candidates: string[];
  columns: string[];
}

/** An output column of the view that is an input column, unchanged. */
export interface PassedColumn {
  column: string;
  source: ColumnRef;
}

export interface ViewJoins {
  pairs: JoinColumnPair[];
  usingJoins: UsingJoin[];
  outputs: PassedColumn[];
}

type TokenKind = "word" | "ident" | "string" | "number" | "op" | "punct";

interface Token {
  kind: TokenKind;
  /** Lower case for words, unescaped for quoted identifiers. */
  text: string;
  depth: number;
}

const OPERATOR_CHARS = new Set("+-*/<>=~!@#%^&|`?:");

function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let depth = 0;
  let i = 0;
  const push = (kind: TokenKind, text: string) =>
    tokens.push({ kind, text, depth });
  while (i < input.length) {
    const ch = input[i];
    if (/\s/.test(ch)) {
      i++;
    } else if (ch === '"' || ch === "'") {
      // `E'...'` strings escape with backslashes, everything else by doubling.
      const backslashEscapes =
        ch === "'" && i > 0 && (input[i - 1] === "E" || input[i - 1] === "e");
      let text = "";
      i++;
      while (i < input.length) {
        if (backslashEscapes && input[i] === "\\") {
          text += input[i + 1] ?? "";
          i += 2;
        } else if (input[i] === ch) {
          if (input[i + 1] === ch) {
            text += ch;
            i += 2;
          } else {
            i++;
            break;
          }
        } else {
          text += input[i++];
        }
      }
      push(ch === '"' ? "ident" : "string", text);
    } else if (/[A-Za-z_]/.test(ch)) {
      let text = "";
      while (i < input.length && /[A-Za-z0-9_$]/.test(input[i])) {
        text += input[i++];
      }
      // `E'...'` lexes as the word `e` followed by a string, which is harmless.
      push("word", text.toLowerCase());
    } else if (/[0-9]/.test(ch)) {
      let text = "";
      while (i < input.length && /[0-9.eE]/.test(input[i])) text += input[i++];
      push("number", text);
    } else if (OPERATOR_CHARS.has(ch)) {
      let text = "";
      while (i < input.length && OPERATOR_CHARS.has(input[i])) {
        text += input[i++];
      }
      push("op", text);
    } else {
      if (ch === ")") depth--;
      push("punct", ch);
      if (ch === "(") depth++;
      i++;
    }
  }
  return tokens;
}

const isWord = (token: Token | undefined, ...words: string[]) =>
  token?.kind === "word" && words.includes(token.text);
const isPunct = (token: Token | undefined, ch: string) =>
  token?.kind === "punct" && token.text === ch;

const CATALOG_ID = /^[a-z]+[0-9]+$/;

interface RelationRef {
  objectId: string;
  /** Index of the token after the reference and its alias. */
  end: number;
  depth: number;
}

/**
 * Reads `[u4 AS "db"."schema"."name"] [AS "alias" [(columns)]]` at `start`.
 * Returns the alias, or null when the alias renames columns, since a column
 * named through it may not be the relation's column of that name.
 */
function readRelationRef(
  tokens: Token[],
  start: number,
): (RelationRef & { alias: string | null }) | null {
  const id = tokens[start + 1];
  if (id?.kind !== "word" || !CATALOG_ID.test(id.text)) return null;
  if (!isWord(tokens[start + 2], "as")) return null;
  let i = start + 3;
  let name: string | null = null;
  while (i < tokens.length && !isPunct(tokens[i], "]")) {
    if (tokens[i].kind === "ident") name = tokens[i].text;
    i++;
  }
  if (name === null || i >= tokens.length) return null;
  i++;
  let alias: string | null = name;
  if (isWord(tokens[i], "as") && tokens[i + 1]?.kind === "ident") {
    alias = tokens[i + 1].text;
    i += 2;
  }
  if (isPunct(tokens[i], "(")) alias = null;
  return {
    objectId: id.text,
    alias,
    end: i,
    depth: tokens[start].depth,
  };
}

/** Words that may precede a join condition: `ON a.x = b.y AND c.z = d.w`. */
const CONDITION_STARTS = new Set(["on", "and", "where", "having"]);

/** Reads `"alias"."column"` ending just before `end`, as a whole operand. */
function operandBefore(tokens: Token[], end: number) {
  const [qualifier, dot, column, before] = [
    tokens[end - 3],
    tokens[end - 2],
    tokens[end - 1],
    tokens[end - 4],
  ];
  if (
    qualifier?.kind !== "ident" ||
    !isPunct(dot, ".") ||
    column?.kind !== "ident"
  ) {
    return null;
  }
  const standsAlone =
    (before?.kind === "word" && CONDITION_STARTS.has(before.text)) ||
    isPunct(before, "(");
  return standsAlone
    ? { qualifier: qualifier.text, column: column.text }
    : null;
}

/** Reads `"alias"."column"` starting at `start`, as a whole operand. */
function operandAfter(tokens: Token[], start: number) {
  const [qualifier, dot, column, after] = [
    tokens[start],
    tokens[start + 1],
    tokens[start + 2],
    tokens[start + 3],
  ];
  if (
    qualifier?.kind !== "ident" ||
    !isPunct(dot, ".") ||
    column?.kind !== "ident"
  ) {
    return null;
  }
  // Anything that continues the expression, such as `+ 1`, `::text` or a
  // third name part, means the operand is not the bare column.
  const continues =
    after?.kind === "op" || isPunct(after, ".") || isPunct(after, "[");
  return continues ? null : { qualifier: qualifier.text, column: column.text };
}

/** The joins in a view definition, as stored in `create_sql`. */
export function extractViewJoins(createSql: string): ViewJoins {
  const tokens = tokenize(createSql);

  const bindings = new Map<string, string>();
  const unusable = new Set<string>();
  const bind = (alias: string, objectId: string) => {
    const bound = bindings.get(alias);
    if (bound !== undefined && bound !== objectId) unusable.add(alias);
    bindings.set(alias, objectId);
  };
  const refs: RelationRef[] = [];
  const refAt = new Map<number, RelationRef>();

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    // Types print as references too, as in `::[s46 AS "pg_catalog"."text"]`,
    // so only a reference in a FROM item position names a relation.
    const inFromPosition =
      isWord(tokens[i - 1], "from", "join", "lateral") ||
      isPunct(tokens[i - 1], ",");
    if (isPunct(token, "[") && inFromPosition) {
      const ref = readRelationRef(tokens, i);
      if (!ref) continue;
      if (ref.alias !== null) bind(ref.alias, ref.objectId);
      refs.push(ref);
      refAt.set(i, ref);
    } else if (
      // A subquery or table function named `) AS "x"`, or a CTE named
      // `WITH "x" AS (` or `, "x" (columns) AS (`. The name may shadow a
      // relation alias elsewhere in the statement, so it resolves nothing.
      isPunct(token, ")") &&
      isWord(tokens[i + 1], "as") &&
      tokens[i + 2]?.kind === "ident"
    ) {
      unusable.add(tokens[i + 2].text);
    } else if (
      (isWord(token, "with", "recursive") || isPunct(token, ",")) &&
      tokens[i + 1]?.kind === "ident" &&
      (isWord(tokens[i + 2], "as") || isPunct(tokens[i + 2], "("))
    ) {
      unusable.add(tokens[i + 1].text);
    }
  }
  const resolve = (alias: string) =>
    unusable.has(alias) ? undefined : bindings.get(alias);

  const pairs: JoinColumnPair[] = [];
  const addPair = (
    left: { qualifier: string; column: string },
    right: { qualifier: string; column: string },
  ) => {
    const leftId = resolve(left.qualifier);
    const rightId = resolve(right.qualifier);
    if (!leftId || !rightId || leftId === rightId) return;
    pairs.push({
      left: { objectId: leftId, column: left.column },
      right: { objectId: rightId, column: right.column },
    });
  };

  const usingJoins: UsingJoin[] = [];
  // Relations joined so far in the FROM clause open at each depth.
  const fromClauses = new Map<number, string[]>();

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.kind === "op" && token.text === "=") {
      const left = operandBefore(tokens, i);
      const right = operandAfter(tokens, i + 1);
      if (left && right) addPair(left, right);
    } else if (isWord(token, "in") && isPunct(tokens[i + 1], "(")) {
      // `a.x IN (SELECT b.y FROM ...)` is a semijoin on x = y.
      const left = operandBefore(tokens, i);
      const right = isWord(tokens[i + 2], "select")
        ? operandAfter(tokens, i + 3)
        : null;
      if (left && right && isWord(tokens[i + 6], "from")) addPair(left, right);
    } else if (isWord(token, "from") && !isWord(tokens[i - 1], "distinct")) {
      fromClauses.set(token.depth, []);
    } else if (refAt.has(i)) {
      const ref = refAt.get(i)!;
      const joined = fromClauses.get(ref.depth);
      if (!joined) continue;
      if (
        isWord(tokens[ref.end], "using") &&
        isPunct(tokens[ref.end + 1], "(")
      ) {
        const columns: string[] = [];
        let j = ref.end + 2;
        while (tokens[j]?.kind === "ident") {
          columns.push(tokens[j].text);
          j += isPunct(tokens[j + 1], ",") ? 2 : 1;
        }
        if (columns.length > 0 && joined.length > 0) {
          usingJoins.push({
            right: ref.objectId,
            candidates: [...new Set(joined)],
            columns,
          });
        }
      }
      joined.push(ref.objectId);
    }
  }

  return { pairs, usingJoins, outputs: passedColumns(tokens, resolve) };
}

/**
 * Output columns of the top-level SELECT that are bare input columns, as in
 * `"o"."id" AS "org_id"`. An unqualified column resolves only when the
 * top-level FROM names a single relation. A set operation at the top level
 * mixes several SELECT lists, so it yields nothing.
 */
function passedColumns(
  tokens: Token[],
  resolve: (alias: string) => string | undefined,
): PassedColumn[] {
  const top = tokens.filter((token) => token.depth === 0);
  if (top.some((token) => isWord(token, "union", "except", "intersect"))) {
    return [];
  }
  const start = tokens.findIndex(
    (token) => token.depth === 0 && isWord(token, "select"),
  );
  if (start === -1) return [];
  let i = start + 1;
  if (isWord(tokens[i], "distinct")) {
    i++;
    if (isWord(tokens[i], "on") && isPunct(tokens[i + 1], "(")) {
      i += 2;
      while (i < tokens.length && tokens[i].depth > 0) i++;
      i++;
    }
  }
  const items: Token[][] = [[]];
  let end = i;
  for (; end < tokens.length; end++) {
    const token = tokens[end];
    if (token.depth === 0 && isWord(token, "from")) break;
    if (token.depth === 0 && isPunct(token, ",")) items.push([]);
    else items[items.length - 1].push(token);
  }

  // The top-level FROM's relation, when it has exactly one item.
  let single: string | undefined;
  let fromItems = 0;
  for (let j = end + 1; j < tokens.length; j++) {
    const token = tokens[j];
    if (token.depth !== 0) continue;
    if (isWord(token, "where", "group", "having", "order", "limit", "offset")) {
      break;
    }
    if (isWord(token, "join") || isPunct(token, ",")) fromItems = Infinity;
    if (isPunct(token, "[") && isWord(tokens[j - 1], "from")) {
      single = readRelationRef(tokens, j)?.objectId;
      fromItems++;
    }
  }
  if (fromItems !== 1) single = undefined;

  const outputs: PassedColumn[] = [];
  for (const item of items) {
    const alias =
      item.length >= 2 &&
      isWord(item[item.length - 2], "as") &&
      item[item.length - 1].kind === "ident"
        ? item[item.length - 1].text
        : null;
    const expression = alias ? item.slice(0, -2) : item;
    let source: ColumnRef | null = null;
    if (
      expression.length === 3 &&
      expression[0].kind === "ident" &&
      isPunct(expression[1], ".") &&
      expression[2].kind === "ident"
    ) {
      const objectId = resolve(expression[0].text);
      if (objectId) source = { objectId, column: expression[2].text };
    } else if (
      expression.length === 1 &&
      expression[0].kind === "ident" &&
      single
    ) {
      source = { objectId: single, column: expression[0].text };
    }
    if (source) outputs.push({ column: alias ?? source.column, source });
  }
  return outputs;
}
