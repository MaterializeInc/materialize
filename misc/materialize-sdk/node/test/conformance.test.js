// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Runs every conformance vector through the binding.

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const { Subscription, conformance } = require("#core");
const { ProtocolError, TimestampTooLarge, UsageError } = require("#errors");

const VECTORS = path.join(__dirname, "..", "..", "conformance");
const CASES = fs
  .readdirSync(VECTORS)
  .filter((file) => file.endsWith(".json"))
  .sort()
  .flatMap((file) => {
    const vectors = JSON.parse(fs.readFileSync(path.join(VECTORS, file)));
    return vectors.cases.map((c) => ({ file, function: vectors.function, c }));
  });

const KINDS = new Map([
  [ProtocolError, "protocol"],
  [TimestampTooLarge, "timestamp_too_large"],
  [UsageError, "usage"],
]);

function kind(error) {
  const name = KINDS.get(error.constructor);
  if (name === undefined) {
    throw error;
  }
  return { error: name };
}

// Runs a `subscription` vector through the binding's own class, the way the
// core's runner does through `binding::Subscription`.
function subscription(input) {
  const { steps, ...options } = input;
  let sub;
  try {
    sub = new Subscription(JSON.stringify(options));
  } catch (error) {
    return kind(error);
  }
  const outputs = [{ declare: sub.declareSql() }];
  for (const step of steps) {
    const [[name, stepInput]] = Object.entries(step);
    try {
      outputs.push(JSON.parse(sub[name](JSON.stringify(stepInput))));
    } catch (error) {
      outputs.push(kind(error));
    }
  }
  return outputs;
}

test("the vectors are found", () => {
  assert.ok(CASES.length > 200);
});

for (const { file, function: fn, c } of CASES) {
  test(`${path.basename(file, ".json")}: ${c.name}`, () => {
    const output =
      fn === "subscription"
        ? subscription(c.input)
        : JSON.parse(conformance(fn, JSON.stringify(c.input)));
    assert.deepEqual(output, c.expect);
  });
}
