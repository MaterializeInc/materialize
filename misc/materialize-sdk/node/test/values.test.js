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

// The generated converters, run over every event and classification in the
// vectors and checked against the schema by a reader separate from the
// generator.

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const { eventFromJson } = require("../dist/values_json.js");

const ROOT = path.join(__dirname, "..", "..");
const DEFS = JSON.parse(
  fs.readFileSync(path.join(ROOT, "schema", "values.schema.json")),
).$defs;

// The properties `schema` gives `value`: its own, those of the `oneOf`
// variant whose `const` matches, and those of a `$ref` beside them.
function properties(schema, value) {
  if (schema.$ref !== undefined && schema.properties === undefined) {
    return properties(DEFS[schema.$ref.split("/").pop()], value);
  }
  let found = { ...schema.properties };
  if (schema.$ref !== undefined) {
    found = {
      ...found,
      ...properties(DEFS[schema.$ref.split("/").pop()], value),
    };
  }
  for (const variant of schema.oneOf ?? []) {
    const tag = Object.entries(variant.properties ?? {}).find(
      ([, p]) => "const" in p,
    );
    if (tag !== undefined && value[tag[0]] === tag[1].const) {
      found = { ...found, ...properties(variant, value) };
    }
  }
  return found;
}

// Checks that `value` holds a bigint where `schema` has a decimal string.
function native(schema, value, at) {
  if (schema.$ref !== undefined && schema.properties === undefined) {
    return native(DEFS[schema.$ref.split("/").pop()], value, at);
  }
  const types = [schema.type].flat();
  if (value === null) {
    assert.ok(types.includes("null"), `${at} is null`);
  } else if (schema.format !== undefined) {
    assert.equal(typeof value, "bigint", `${at} is a bigint`);
  } else if (types.includes("string")) {
    assert.equal(typeof value, "string", `${at} is a string`);
  } else if (types.includes("array")) {
    value.forEach((item, i) => native(schema.items, item, `${at}[${i}]`));
  } else if (typeof value === "object") {
    const props = properties(schema, value);
    assert.deepEqual(Object.keys(value).sort(), Object.keys(props).sort(), at);
    for (const [key, item] of Object.entries(value)) {
      native(props[key], item, `${at}.${key}`);
    }
  }
}

const json = (value) =>
  JSON.parse(
    JSON.stringify(value, (_, v) => (typeof v === "bigint" ? v.toString() : v)),
  );

function* events(value) {
  if (Array.isArray(value)) {
    for (const item of value) yield* events(item);
  } else if (value !== null && typeof value === "object") {
    if ("event" in value) yield value;
    for (const item of Object.values(value)) yield* events(item);
  }
}

test("every event in the vectors converts to bigints where the schema has integers", () => {
  let count = 0;
  for (const file of fs.readdirSync(path.join(ROOT, "conformance"))) {
    if (!file.endsWith(".json")) continue;
    const vectors = JSON.parse(
      fs.readFileSync(path.join(ROOT, "conformance", file)),
    );
    for (const c of vectors.cases) {
      for (const event of events(c.expect)) {
        const converted = eventFromJson(event);
        native(DEFS.Event, converted, `${file}: ${c.name}`);
        assert.deepEqual(json(converted), event);
        count += 1;
      }
    }
  }
  assert.ok(count >= 45, `only ${count} events`);
});

test("an unknown variant is refused", () => {
  assert.throws(
    () => eventFromJson({ event: "other" }),
    /unknown event: other/,
  );
});
