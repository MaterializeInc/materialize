# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The generated converters, run over every event in the vectors and checked
against the schema by a reader separate from the generator."""

import json
from pathlib import Path
from typing import Any

import pytest
from materialize_sdk._values import event_from_json

ROOT = Path(__file__).resolve().parents[2]
DEFS = json.loads((ROOT / "schema" / "values.schema.json").read_text())["$defs"]


def resolve(schema: dict[str, Any]) -> dict[str, Any]:
    if "$ref" in schema and "properties" not in schema:
        return resolve(DEFS[schema["$ref"].split("/")[-1]])
    return schema


def properties(schema: dict[str, Any], value: dict[str, Any]) -> dict[str, Any]:
    """The properties ``schema`` gives ``value``: its own, those of the
    ``oneOf`` variant whose ``const`` matches, and those of a ``$ref``
    beside them."""
    schema = resolve(schema)
    found = dict(schema.get("properties", {}))
    if "$ref" in schema:
        found |= properties(DEFS[schema["$ref"].split("/")[-1]], value)
    for variant in schema.get("oneOf", []):
        tags = [
            (k, p["const"])
            for k, p in variant.get("properties", {}).items()
            if "const" in p
        ]
        if tags and value.get(tags[0][0]) == tags[0][1]:
            found |= properties(variant, value)
    return found


def native(schema: dict[str, Any], value: Any, at: str) -> None:
    """Checks that ``value`` holds an ``int`` where ``schema`` has a decimal
    string."""
    schema = resolve(schema)
    types = schema.get("type")
    types = types if isinstance(types, list) else [types]
    if value is None:
        assert "null" in types, f"{at} is None"
    elif "format" in schema:
        assert type(value) is int, f"{at} is an int"
    elif "string" in types:
        assert isinstance(value, str), f"{at} is a str"
    elif "array" in types:
        for i, item in enumerate(value):
            native(schema["items"], item, f"{at}[{i}]")
    elif isinstance(value, dict):
        props = properties(schema, value)
        assert sorted(value) == sorted(props), at
        for key, item in value.items():
            native(props[key], item, f"{at}.{key}")


def events(value: Any) -> list[Any]:
    if isinstance(value, list):
        return [e for item in value for e in events(item)]
    if isinstance(value, dict):
        found = [value] if "event" in value else []
        return found + [e for item in value.values() for e in events(item)]
    return []


def strings(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return [strings(v) for v in value]
    if isinstance(value, dict):
        return {k: strings(v) for k, v in value.items()}
    return value


def test_every_event_in_the_vectors_converts_to_ints_where_the_schema_has_integers():
    count = 0
    for path in sorted((ROOT / "conformance").glob("*.json")):
        for case in json.loads(path.read_text())["cases"]:
            for event in events(case["expect"]):
                converted = event_from_json(event)
                native(DEFS["Event"], converted, f"{path.stem}: {case['name']}")
                assert strings(converted) == event
                count += 1
    assert count >= 45, f"only {count} events"


def test_an_unknown_variant_is_refused():
    with pytest.raises(ValueError, match="unknown event: 'other'"):
        event_from_json({"event": "other"})
