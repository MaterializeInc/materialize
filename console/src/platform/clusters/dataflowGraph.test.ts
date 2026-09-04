// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Channel,
  LirOperator,
  Operator,
} from "~/api/materialize/useDataflowStructure";

import { collateOperators, scopeToGv } from "./dataflowGraph";

/**
 * A catalog string shaped to break out of a DOT quoted string and add an
 * `href` attribute, which Graphviz renders as an anchor around the node. The
 * leading backslash is the payload: an escaper that rewrites `"` but not `\`
 * turns it into `\\`, which Graphviz reads as one literal backslash, leaving
 * the quote that follows free to close the label.
 */
const BREAKOUT_PAYLOAD =
  'foo\\", href=<javascript:alert(1)>, label=<click>];}//';

/** An arrangement operator name as Materialize actually formats it. */
const ARRANGE_BY_NAME = 'ArrangeBy[[Column(0, "id")]]';

interface ScannedDot {
  /** Text in DOT syntax position, outside any quoted string. */
  outside: string;
  /** Decoded contents of each quoted string. */
  strings: string[];
  /**
   * Whether the input ended inside a quoted string. A payload that breaks out
   * of a label consumes the quote that was meant to close it, so the quotes
   * that follow shift by one and the scan runs off the end.
   */
  unterminated: boolean;
}

/**
 * Splits `dot` into the text outside quoted strings and the decoded contents
 * of each quoted string, applying DOT's own rules: within a quoted string a
 * backslash escapes the following character, so neither `\\` nor `\"` can
 * terminate the string.
 *
 * This mirrors how Graphviz decides where a string ends, which is the property
 * the escaping has to get right. Asserting on the halves separately lets a test
 * say "this text stayed data" rather than merely "this text is present".
 */
function scanDot(dot: string): ScannedDot {
  const strings: string[] = [];
  let outside = "";
  let current: string | null = null;
  for (let i = 0; i < dot.length; i++) {
    const character = dot[i];
    if (current === null) {
      if (character === '"') {
        current = "";
      } else {
        outside += character;
      }
    } else if (character === "\\") {
      current += dot[i + 1] ?? "";
      i++;
    } else if (character === '"') {
      strings.push(current);
      current = null;
    } else {
      current += character;
    }
  }
  if (current !== null) {
    strings.push(current);
  }
  return { outside, strings, unterminated: current !== null };
}

/**
 * Asserts that `injected` reached the output whole and entirely inside a
 * quoted string, leaving no DOT syntax outside one.
 */
function expectInert(dot: string, injected: string) {
  const { outside, strings, unterminated } = scanDot(dot);
  expect(unterminated).toBe(false);
  expect(strings).toContain(injected);
  // An escaped-out payload would show up here as bare attributes.
  expect(outside).not.toContain("href");
  expect(outside).not.toContain("javascript");
  // Graphviz reads `<...>` as an HTML-like string, an attribute value that
  // needs no quotes at all, so no unquoted angle bracket may ever appear.
  expect(outside).not.toContain("<");
}

const ROOT: Operator = {
  id: 1n,
  address: ["1"],
  name: "root",
  parentId: null,
  arrangementRecords: null,
  arrangementSizes: null,
  elapsedNs: 0,
  lirId: null,
  lirOperator: null,
};

function childOperator(overrides: Partial<Operator> = {}): Operator {
  return {
    id: 2n,
    address: ["1", "1"],
    name: "Reduce",
    parentId: 1n,
    arrangementRecords: null,
    arrangementSizes: null,
    elapsedNs: 0,
    lirId: "1",
    lirOperator: "Get::PassArrangements materialize.public.t",
    ...overrides,
  };
}

function channel(overrides: Partial<Channel> = {}): Channel {
  return {
    id: 1,
    fromOperatorId: 2,
    fromOperatorAddress: ["1", "1"],
    fromPort: 0,
    toOperatorId: 3,
    toOperatorAddress: ["1", "2"],
    toPort: 0,
    messagesSent: 5,
    batchesSent: 1,
    channelType: "Exchange",
    ...overrides,
  };
}

function render(
  operators: Operator[],
  channels: Channel[] = [],
  lirOperators: LirOperator[] = [],
): string {
  const [, root, enrichedLirOperators] = collateOperators(
    operators,
    channels,
    lirOperators,
  );
  return scopeToGv(root, enrichedLirOperators);
}

describe("scanDot", () => {
  it("reports a payload that escaped its quoted string", () => {
    // What escaping only `"` produces: the payload's own backslash pairs with
    // the inserted one to form `\\`, a literal backslash, so the label ends at
    // `foo\` and the rest of the payload lands in DOT syntax position. Without
    // this case the assertions above could pass against a broken escaper.
    const quoteOnlyEscaped = BREAKOUT_PAYLOAD.replace(/"/g, '\\"');
    const vulnerable = `digraph {\n"[1,1]" [label="${quoteOnlyEscaped}",class=""];\n}`;

    const { outside, strings, unterminated } = scanDot(vulnerable);

    expect(strings).not.toContain(BREAKOUT_PAYLOAD);
    expect(strings).toContain("foo\\");
    expect(outside).toContain("href");
    expect(outside).toContain("javascript");
    expect(outside).toContain("<");
    expect(unterminated).toBe(true);
  });
});

describe("scopeToGv", () => {
  it("renders an operator name that breaks out of a DOT label as inert text", () => {
    const dot = render([ROOT, childOperator({ name: BREAKOUT_PAYLOAD })]);

    expectInert(dot, BREAKOUT_PAYLOAD);
  });

  it("renders a breakout payload in the node tooltip as inert text", () => {
    const dot = render([
      ROOT,
      childOperator({ lirId: "1", lirOperator: BREAKOUT_PAYLOAD }),
    ]);

    expectInert(dot, `Lir ID 1: ${BREAKOUT_PAYLOAD}`);
  });

  it("renders a breakout payload in the channel tooltip as inert text", () => {
    const dot = render(
      [ROOT, childOperator()],
      [channel({ channelType: BREAKOUT_PAYLOAD })],
    );

    expectInert(dot, BREAKOUT_PAYLOAD);
  });

  it("renders a breakout payload in the LIR subgraph label as inert text", () => {
    const dot = render(
      [ROOT, childOperator()],
      [],
      [{ lir_id: "7", operator: BREAKOUT_PAYLOAD, addresses: [["1", "1"]] }],
    );

    expectInert(dot, `LIR ID 7: ${BREAKOUT_PAYLOAD}`);
    // The subgraph must still be a cluster, which Graphviz decides from the
    // name it reads after stripping the quotes.
    expect(scanDot(dot).strings).toContain("cluster_7");
  });

  it("preserves quotes in arrangement operator names", () => {
    const dot = render([ROOT, childOperator({ name: ARRANGE_BY_NAME })]);

    expect(scanDot(dot).strings).toContain(ARRANGE_BY_NAME);
  });

  it("escapes a trailing backslash so it cannot consume the closing quote", () => {
    const dot = render([
      ROOT,
      childOperator({ name: "ends with a backslash\\" }),
    ]);

    expect(scanDot(dot).strings).toContain("ends with a backslash\\");
  });

  it("dashes channels that have carried no messages", () => {
    const idle = render(
      [ROOT, childOperator()],
      [channel({ messagesSent: 0, batchesSent: 0 })],
    );
    const active = render([ROOT, childOperator()], [channel()]);

    expect(idle).toContain(',style="dashed"];');
    expect(active).not.toContain('style="dashed"');
  });

  it("labels regions so the visualizer can bind a drill-down handler", () => {
    const dot = render([
      ROOT,
      childOperator(),
      { ...childOperator(), id: 3n, address: ["1", "1", "1"], parentId: 2n },
    ]);

    expect(dot).toContain('class="region"');
  });
});
