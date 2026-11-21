# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Kafka sources in the formats beyond text: JSON, JSON with undecodable
values, and Avro.

Each is a `kafka.KafkaSource` with a topic and a Kafka source of its own, so
they share its reasoning: an export is compared with the topic's records
below the offsets the source's progress maps.

* `JsonSource` ingests documents in many spellings as `jsonb` and compares
  them by value.
* `RecoverySource` mixes documents, undecodable values and tombstones per
  key. `upsert_recovery_model` says which keys are in a decode error state
  below the mapped offsets. An upsert export must fail to read exactly while
  there is one, and a later document or tombstone must heal it. A second
  export of the same topic reports the errors inline instead.
* `AvroSource` writes records in the schema registry's wire format. Every
  field of a record is a pure function of its `seed` field, so each visible
  row is also checked on its own, whatever prefix of the topic is ingested.

`KafkaFormats` drives them as one component.
"""

from __future__ import annotations

import json
import math
import random
import struct
import uuid
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import sink_oracle as oracle
from materialize.antithesis import upsert_recovery_model as model
from materialize.antithesis_driver.client import Status
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw
from materialize.antithesis_driver.kafka import (
    PARTITIONS,
    KafkaSource,
    Produced,
    Record,
    Registry,
    Tailer,
    partition_for,
)

JSON_STRINGS = (
    "",
    "a",
    "héllo",
    'q"uote',
    "back\\slash",
    "line\nbreak\ttab",
    "😀",
    "NULL",
    " spaced ",
)
UNDECODABLE = (
    b'{"a":',
    b"nope",
    b'{"a":1}}',
    b" ",
    b'{"a":1} {"b":2}',
    b"{'a':1}",
    b"[1,2,]",
    b"NaN",
    b'{"a": NaN}',
    b"01",
    b"tru",
    b'\xef\xbb\xbf{"a":1}',
    b'\xff\xfe{"a":1}',
)
"""Values that neither Materialize nor `upsert_recovery_model.canonical`
decodes as JSON, checked against v26.51.0. Not every text qualifies: Python
accepts `1e99999`, which overflows `numeric`."""


def json_document(r: random.Random, depth: int = 0) -> str:
    """A JSON text in one of many spellings of its value: number forms,
    escapes, repeated keys and whitespace vary.

    Numbers stay below the 39 digits of `numeric`, which `jsonb` stores them
    as, so no spelling loses precision.
    """
    kinds = ("number", "number", "string", "literal")
    kind = r.choice(kinds + ("object", "array") if depth < 3 else kinds)
    if kind == "number":
        whole = r.choice(
            (0, 1, -1, r.randint(-(10**6), 10**6), r.randint(-(10**28), 10**28))
        )
        form = r.choice(("int", "decimal", "exponent", "negative zero"))
        if form == "int":
            return str(whole)
        if form == "decimal":
            return f"{whole}.{r.randint(0, 99999):05d}"
        if form == "exponent":
            return f"{r.randint(-999, 999)}{r.choice('eE')}{r.randint(-9, 9)}"
        return r.choice(("-0", "-0.0"))
    if kind == "string":
        return json.dumps(r.choice(JSON_STRINGS), ensure_ascii=r.random() < 0.5)
    if kind == "literal":
        return r.choice(("true", "false", "null"))
    pad = r.choice(("", " ", "\n"))
    if kind == "array":
        items = (json_document(r, depth + 1) for _ in range(r.randint(0, 4)))
        return "[" + f",{pad}".join(items) + "]"
    members = (
        f"{json.dumps(r.choice('abcd'))}{pad}:{pad}{json_document(r, depth + 1)}"
        for _ in range(r.randint(0, 4))
    )
    return "{" + f"{pad},".join(members) + "}"


class JsonSource(KafkaSource):
    """Documents ingested as `jsonb`, as an append-only export with the key."""

    KIND = "json"
    SOURCE = "kafkasrc.json_src"

    def setup_statements(self) -> list[str]:
        return [
            *self._source_statements(),
            self._table(
                "json_none",
                "KEY FORMAT TEXT VALUE FORMAT JSON INCLUDE KEY, PARTITION, OFFSET"
                " ENVELOPE NONE",
            ),
        ]

    def produce(self, d: Draw) -> Status:
        if self._producer is None:
            return Status.NOT_SENT
        r = random.Random(d.integer(0, 2**40))
        records = []
        for _ in range(d.choice((1, 10, 50))):
            pad = r.choice(("", " ", "\n"))
            document = f"{pad}{json_document(r)}{pad}"
            key = f"k{d.integer(0, self.keyspace - 1)}"
            records.append(self._record(key, document.encode()))
        return self._send(records)

    def _exports(self) -> list[tuple[str, list[str], str]]:
        columns = ["key", "data::text", "partition::text", '"offset"::text']
        return [("n", columns, "kafkasrc.json_none")]

    def _judge_exports(
        self, base: dict[str, Any], below: list[Record], rows: list[tuple[Any, ...]]
    ) -> None:
        def canonical(text: str | None) -> str:
            try:
                return model.canonical(text or "")
            except ValueError:
                return f"not JSON: {text!r}"

        expected = Counter(
            (
                (r.key or b"").decode(),
                canonical(r.value.decode()),
                str(r.partition),
                str(r.offset),
            )
            for r in below
            if r.value is not None
        )
        observed = Counter(
            (r[2], canonical(r[3]), r[4], r[5]) for r in rows if r[1] == "n"
        )
        always(
            expected == observed,
            "kafka source: the JSON none-envelope export equals the topic documents below the mapped offsets",
            {**base, **oracle.bag_diff(expected.elements(), observed.elements())},
        )
        sometimes(
            len(expected) > 0 and expected == observed,
            "kafka source: a check matched a nonempty JSON export",
            {**base, "documents": sum(expected.values())},
        )


class RecoverySource(KafkaSource):
    """Upsert exports of a topic whose values do not always decode."""

    KIND = "rec"
    SOURCE = "kafkarec.src"
    """In a schema of its own: a check reads in a transaction, whose time
    domain is every relation of the schemas it touches."""
    STRICT = "SELECT key, data::text FROM kafkarec.strict"

    def __init__(self, ctx: Context) -> None:
        super().__init__(ctx)
        self.poisoned: set[str] = set()
        """Keys whose last value sent was undecodable. Only steers repairs:
        the topic decides what a check expects."""
        self.poison_share = 0.1
        self.repair_share = 0.2

    def configure(self, d: Draw) -> None:
        self.keyspace = d.choice((1, 4, 16))
        self.tombstone_share = d.choice((0.05, 0.3))
        self.poison_share = d.choice((0.05, 0.2))
        self.repair_share = d.choice((0.1, 0.3))

    def setup_statements(self) -> list[str]:
        upsert = "KEY FORMAT TEXT VALUE FORMAT JSON ENVELOPE UPSERT"
        return [
            *self._source_statements(),
            self._table("strict", upsert),
            self._table("inline", f"{upsert} (VALUE DECODING ERRORS = INLINE)"),
        ]

    def produce(self, d: Draw) -> Status:
        if self._producer is None:
            return Status.NOT_SENT
        r = random.Random(d.integer(0, 2**40))

        def healthy() -> bytes | None:
            if d.boolean(self.tombstone_share):
                return None
            return json_document(r).encode()

        values: dict[str, bytes | None] = {}
        with self._lock:
            poisoned = sorted(self.poisoned)
        if poisoned and d.boolean(self.repair_share):
            values = {key: healthy() for key in poisoned}
        elif d.boolean(self.poison_share):
            for _ in range(d.choice((1, 2))):
                key = f"k{d.integer(0, self.keyspace - 1)}"
                values[key] = d.choice(UNDECODABLE)
        else:
            for _ in range(d.choice((1, 5, 20))):
                values[f"k{d.integer(0, self.keyspace - 1)}"] = healthy()
        status = self._send([self._record(k, v) for k, v in values.items()])
        if status != Status.NOT_SENT:
            with self._lock:
                for key, value in values.items():
                    if value in UNDECODABLE:
                        self.poisoned.add(key)
                    else:
                        self.poisoned.discard(key)
        return status

    def _exports(self) -> list[tuple[str, list[str], str]]:
        return [("i", ["key", "data::text", "error::text"], "kafkarec.inline")]

    def _read(self, conn: psycopg.Connection) -> list[tuple[Any, ...]]:
        """The progress and the inline export, then the strict export at the
        same timestamp: both statements of one transaction read at one time.

        The strict read ends the result with an `s` row per key and an `ok`
        row, or with one `e` row when it failed on a decode error.
        """
        conn.execute(b"BEGIN")
        rows = conn.execute(self._query().encode()).fetchall()
        ts = rows[0][0] if rows else None
        try:
            strict = conn.execute(self.STRICT.encode()).fetchall()
        except psycopg.Error as e:
            if e.sqlstate != "XX000" or "Failed to decode JSON" not in str(e):
                raise
            conn.execute(b"ROLLBACK")
            return [*rows, (ts, "e", str(e).strip().splitlines()[0][:300])]
        conn.execute(b"COMMIT")
        return [*rows, *((ts, "s", key, text) for key, text in strict), (ts, "ok")]

    def _judge_exports(
        self, base: dict[str, Any], below: list[Record], rows: list[tuple[Any, ...]]
    ) -> None:
        records = [
            model.Record(
                r.partition,
                r.offset,
                (r.key or b"").decode(),
                None if r.value is None else r.value.decode("utf8", "replace"),
            )
            for r in below
        ]
        _, errors = model.expected(records)
        failed = [r[2] for r in rows if r[1] == "e"]
        strict = [(r[2], r[3]) for r in rows if r[1] == "s"]
        problems = model.validate(records, None if failed else strict, bool(failed))
        details = {**base, "error_keys": sorted(errors)[:10], "read_error": failed}
        always(
            not problems,
            "kafka source: the JSON upsert export equals the latest document per key below the mapped offsets, and fails to read exactly while one is undecodable",
            {**details, "problems": problems[:5]},
        )
        inline = model.validate_inline(
            records, [(r[2], r[3], r[4]) for r in rows if r[1] == "i"]
        )
        always(
            not inline,
            "kafka source: the JSON upsert export with inline errors shows the latest document or the decode error of each key below the mapped offsets",
            {**details, "problems": inline[:5]},
        )
        sometimes(
            bool(errors) and not problems,
            "kafka source: a JSON upsert read failed on an undecodable latest value",
            details,
        )
        broken = set()
        for record in records:
            if record.value is not None:
                try:
                    model.canonical(record.value)
                except ValueError:
                    broken.add(record.key)
        sometimes(
            bool(broken) and not errors and not problems and not inline,
            "kafka source: a later document or tombstone healed every JSON decode error",
            {**base, "healed_keys": sorted(broken)[:10]},
        )


AVRO_KEY_SCHEMA = {
    "type": "record",
    "name": "key",
    "fields": [{"name": "k", "type": "string"}],
}
AVRO_FIELDS: list[tuple[str, Any, str]] = [
    ("i", "int", "exact"),
    ("l", "long", "exact"),
    ("f", "float", "float4"),
    ("d", "double", "float8"),
    ("b", "boolean", "bool"),
    ("s", "string", "exact"),
    ("by", "bytes", "bytes"),
    ("ns", ["null", "string"], "exact"),
    ("nl", ["null", "long"], "exact"),
    (
        "dec",
        {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 4},
        "decimal",
    ),
    ("dt", {"type": "int", "logicalType": "date"}, "date"),
    ("tms", {"type": "long", "logicalType": "timestamp-millis"}, "millis"),
    ("tmu", {"type": "long", "logicalType": "timestamp-micros"}, "micros"),
    ("u", {"type": "string", "logicalType": "uuid"}, "exact"),
    ("arr", {"type": "array", "items": "long"}, "list"),
    ("m", {"type": "map", "values": "long"}, "map"),
    (
        "rec",
        {
            "type": "record",
            "name": "inner",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": ["null", "string"]},
            ],
        },
        "record",
    ),
    (
        "e",
        {"type": "enum", "name": "color", "symbols": ["RED", "GREEN", "BLUE"]},
        "exact",
    ),
    ("fx", {"type": "fixed", "name": "four", "size": 4}, "bytes"),
]
"""Each value field: its name, its Avro type, and how `shows` compares the
column's text with the value."""
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FLOAT4_EDGES = (
    math.nan,
    math.inf,
    -math.inf,
    -0.0,
    3.4028234663852886e38,
    1.401298464324817e-45,
)
FLOAT8_EDGES = (
    math.nan,
    math.inf,
    -math.inf,
    -0.0,
    1.7976931348623157e308,
    5e-324,
)
TEXT_ALPHABET = "abc XYZ09_-'\"\\\n\té😀"


def avro_value_schema(evolved: bool) -> dict[str, Any]:
    """The value schema. The evolved one adds a field with a default, which
    a reader of the first schema skips."""
    fields: list[dict[str, Any]] = [{"name": "seed", "type": "long"}]
    if evolved:
        fields.append({"name": "extra", "type": "string", "default": ""})
    fields += [{"name": name, "type": type_} for name, type_, _ in AVRO_FIELDS]
    return {"type": "record", "name": "envelope", "fields": fields}


def float4(value: float) -> float:
    return struct.unpack("f", struct.pack("f", value))[0]


def avro_datum(seed: int) -> dict[str, Any]:
    """The value record of `seed`: every field is a pure function of it.

    Dates and timestamps are their Avro integers: days, milliseconds and
    microseconds since the epoch.
    """
    r = random.Random(seed)

    def edge(edges: tuple[Any, ...], draw: Any) -> Any:
        return r.choice(edges) if r.random() < 0.25 else draw()

    def text() -> str:
        return "".join(r.choice(TEXT_ALPHABET) for _ in range(r.randint(0, 12)))

    def int64() -> int:
        return r.randint(-(2**63), 2**63 - 1)

    def int32() -> int:
        return r.randint(-(2**31), 2**31 - 1)

    unscaled = edge(
        (-(10**20) + 1, 10**20 - 1, 0, 1),
        lambda: r.randint(-(10**20) + 1, 10**20 - 1),
    )
    return {
        "seed": seed,
        "i": edge((-(2**31), 2**31 - 1, 0), int32),
        "l": edge((-(2**63), 2**63 - 1, 0), int64),
        "f": float4(edge(FLOAT4_EDGES, lambda: r.uniform(-1e6, 1e6))),
        "d": edge(FLOAT8_EDGES, lambda: r.uniform(-1e15, 1e15)),
        "b": r.random() < 0.5,
        "s": edge(("", " ", "NULL"), text),
        "by": bytes(r.getrandbits(8) for _ in range(r.randint(0, 8))),
        "ns": None if r.random() < 0.3 else text(),
        "nl": None if r.random() < 0.3 else int64(),
        "dec": Decimal(unscaled).scaleb(-4),
        "dt": edge((0, -1, 100_000), lambda: r.randint(-100_000, 100_000)),
        "tms": edge((0, -1), lambda: r.randint(-4 * 10**12, 4 * 10**12)),
        "tmu": edge((0, -1), lambda: r.randint(-4 * 10**15, 4 * 10**15)),
        "u": str(uuid.UUID(int=r.getrandbits(128))),
        "arr": [int64() for _ in range(r.randint(0, 4))],
        "m": {k: r.randint(-1000, 1000) for k in r.sample("abcde", r.randint(0, 3))},
        "rec": {
            "a": int32(),
            # Letters only, so the record's text needs no quoting.
            "b": None if r.random() < 0.3 else r.choice(("x", "abc", "zzzzz")),
        },
        "e": r.choice(("RED", "GREEN", "BLUE")),
        "fx": bytes(r.getrandbits(8) for _ in range(4)),
    }


def shows(kind: str, text: str | None, value: Any) -> bool:
    """Whether `text`, a column cast to text, shows the Avro `value`."""
    if value is None or text is None:
        return value is None and text is None
    try:
        if kind in ("float4", "float8"):
            seen = float4(float(text)) if kind == "float4" else float(text)
            return seen == value or (math.isnan(seen) and math.isnan(value))
        if kind == "decimal":
            return Decimal(text) == value
        if kind == "date":
            return text == (date(1970, 1, 1) + timedelta(days=value)).isoformat()
        if kind in ("millis", "micros"):
            # A `timestamp` column's text carries no zone.
            instant = datetime.fromisoformat(text).replace(tzinfo=UTC)
        if kind == "millis":
            return instant == EPOCH + timedelta(milliseconds=value)
        if kind == "micros":
            return instant == EPOCH + timedelta(microseconds=value)
    except (ValueError, ArithmeticError):
        return False
    if kind == "bytes":
        return text == "\\x" + value.hex()
    if kind == "bool":
        return text == ("true" if value else "false")
    if kind == "list":
        return text == "{" + ",".join(str(item) for item in value) + "}"
    if kind == "map":
        return text == "{" + ",".join(f"{k}=>{value[k]}" for k in sorted(value)) + "}"
    if kind == "record":
        return text == f"({value['a']},{value['b'] or ''})"
    return text == str(value)


def unfaithful(seed_text: str | None, texts: tuple[Any, ...]) -> list[str]:
    """The fields of an Avro export row that do not show what its seed
    determines. `texts` are the value columns in `AVRO_FIELDS` order."""
    try:
        datum = avro_datum(int(seed_text or ""))
    except ValueError:
        return ["seed"]
    return [
        name
        for (name, _, kind), text in zip(AVRO_FIELDS, texts)
        if not shows(kind, text, datum[name])
    ]


class AvroSource(KafkaSource):
    """Avro records with a broad set of field types, as an append-only and
    an upsert export decoded with the registry's schemas."""

    KIND = "avro"
    SOURCE = "kafkasrc.avro_src"

    def __init__(self, ctx: Context) -> None:
        super().__init__(ctx)
        url = ctx.config.schema_registry_url
        assert url is not None
        self.registry = Registry(url)
        self.key_id = 0
        self.value_ids: list[int] = []
        """The value schema the exports were created with, then the evolved
        one once it is registered."""
        self._decoded: dict[tuple[int, int], tuple[str, int | None, bool]] = {}
        self._faithful: set[tuple[Any, ...]] = set()
        """Rows already found faithful. A check reads every row again."""

    def setup_upstream(self) -> None:
        super().setup_upstream()
        self.key_id = self.registry.register(f"{self.topic}-key", AVRO_KEY_SCHEMA)
        value_id = self.registry.register(
            f"{self.topic}-value", avro_value_schema(False)
        )
        with self._lock:
            self.value_ids = self.value_ids or [value_id]

    def setup_statements(self) -> list[str]:
        avro = "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION kafkasrc.csr"
        return [
            *self._source_statements(),
            "CREATE CONNECTION IF NOT EXISTS kafkasrc.csr TO CONFLUENT SCHEMA REGISTRY"
            f" (URL '{self.ctx.config.schema_registry_mz_url}')",
            self._table("avro_none", f"{avro} INCLUDE PARTITION, OFFSET ENVELOPE NONE"),
            self._table("avro_upsert", f"{avro} ENVELOPE UPSERT"),
        ]

    def produce(self, d: Draw) -> Status:
        if self._producer is None:
            return Status.NOT_SENT
        with self._lock:
            value_ids = list(self.value_ids)
        records = []
        for _ in range(d.choice((1, 4, 16))):
            key = f"k{d.integer(0, self.keyspace - 1)}"
            value = None
            if not d.boolean(self.tombstone_share):
                seed = d.integer(0, 2**40)
                datum = avro_datum(seed)
                schema_id = d.choice(value_ids)
                if schema_id != value_ids[0]:
                    datum["extra"] = f"x{seed}"
                value = self.registry.encode(schema_id, datum)
            records.append(
                Produced(
                    self.registry.encode(self.key_id, {"k": key}),
                    value,
                    partition_for(key, PARTITIONS),
                )
            )
        return self._send(records)

    def evolve(self, d: Draw) -> Status:
        """Register a second, compatible value schema, once. Later produces
        write with either. The exports keep reading with the first."""
        with self._lock:
            evolved = len(self.value_ids) > 1
        if self._producer is None or evolved:
            return Status.NOT_SENT

        def action() -> None:
            try:
                schema_id = self.registry.register(
                    f"{self.topic}-value", avro_value_schema(True)
                )
            except OSError as e:
                log(f"kafka avro: registering the evolved schema: {e}")
                return
            with self._lock:
                self.value_ids = [self.value_ids[0], schema_id]
            reachable(
                "kafka source: an evolved Avro value schema was registered",
                {"schema_id": schema_id},
            )

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def _exports(self) -> list[tuple[str, list[str], str]]:
        fields = ["seed::text", *(f"{name}::text" for name, _, _ in AVRO_FIELDS)]
        return [
            ("n", ["partition::text", '"offset"::text', *fields], "kafkasrc.avro_none"),
            ("u", ["k", *fields], "kafkasrc.avro_upsert"),
        ]

    def _decode(self, r: Record) -> tuple[str, int | None, bool]:
        """A record's key, its seed unless it is a tombstone, and whether the
        evolved schema wrote it."""
        cached = self._decoded.get((r.partition, r.offset))
        if cached is None:
            assert r.key is not None
            key = self.registry.decode(r.key)[1]["k"]
            if r.value is None:
                cached = (key, None, False)
            else:
                schema_id, datum = self.registry.decode(r.value)
                cached = (key, datum["seed"], schema_id != self.value_ids[0])
            self._decoded[(r.partition, r.offset)] = cached
        return cached

    def _judge_exports(
        self, base: dict[str, Any], below: list[Record], rows: list[tuple[Any, ...]]
    ) -> None:
        decoded = [(r, *self._decode(r)) for r in below]
        none_rows = [r[2:] for r in rows if r[1] == "n"]
        upsert_rows = [r[2:] for r in rows if r[1] == "u"]
        expected_none = Counter(
            (str(r.partition), str(r.offset), str(seed))
            for r, _, seed, _ in decoded
            if seed is not None
        )
        observed_none = Counter((r[0], r[1], r[2]) for r in none_rows)
        always(
            expected_none == observed_none,
            "kafka source: the Avro none-envelope export equals the topic records below the mapped offsets",
            {
                **base,
                **oracle.bag_diff(expected_none.elements(), observed_none.elements()),
            },
        )
        latest: dict[str, int | None] = {}
        for r, key, seed, _ in sorted(
            decoded, key=lambda x: (x[0].partition, x[0].offset)
        ):
            latest[key] = seed
        expected_upsert = {k: str(v) for k, v in latest.items() if v is not None}
        keys = Counter(r[0] for r in upsert_rows)
        always(
            all(n == 1 for n in keys.values()),
            "kafka source: the Avro upsert export holds at most one row per key",
            {**base, "duplicates": sorted(k for k, n in keys.items() if n > 1)[:10]},
        )
        observed_upsert = {r[0]: r[1] for r in upsert_rows}
        always(
            observed_upsert == expected_upsert,
            "kafka source: the Avro upsert export equals the latest record per key below the mapped offsets",
            {
                **base,
                **oracle.bag_diff(expected_upsert.items(), observed_upsert.items()),
            },
        )
        wrong = []
        for export, start, export_rows in (
            ("none", 2, none_rows),
            ("upsert", 1, upsert_rows),
        ):
            for row in export_rows:
                value = tuple(row[start : start + 1 + len(AVRO_FIELDS)])
                if value in self._faithful:
                    continue
                fields = unfaithful(value[0], value[1:])
                if fields:
                    wrong.append({"export": export, "seed": value[0], "fields": fields})
                else:
                    self._faithful.add(value)
        always(
            not wrong,
            "kafka source: every Avro export row carries the field values its seed determines",
            {**base, "wrong": wrong[:5]},
        )
        sometimes(
            len(expected_upsert) > 0
            and observed_upsert == expected_upsert
            and expected_none == observed_none
            and not wrong,
            "kafka source: a check matched nonempty Avro exports",
            {**base, "keys": len(expected_upsert)},
        )
        sometimes(
            any(evolved for _, _, _, evolved in decoded) and not wrong,
            "kafka source: a check covered Avro records written with an evolved schema",
            base,
        )


class KafkaFormats:
    """The sources above, driven as one component: a rule draws which one."""

    def __init__(self, ctx: Context) -> None:
        self.avro = AvroSource(ctx) if ctx.config.schema_registry_url else None
        self.sources: list[KafkaSource] = [JsonSource(ctx), RecoverySource(ctx)]
        if self.avro is not None:
            self.sources.append(self.avro)

    @property
    def tail(self) -> Tailer | None:
        return self.sources[0].tail

    def configure(self, d: Draw) -> None:
        for source in self.sources:
            source.configure(d)

    def setup_upstream(self) -> None:
        for source in self.sources:
            source.setup_upstream()

    def setup_statements(self) -> list[str]:
        return [s for source in self.sources for s in source.setup_statements()]

    def start(self) -> None:
        for source in self.sources:
            source.start()

    def produce(self, d: Draw) -> Status:
        return d.choice(self.sources).produce(d)

    def check(self, d: Draw) -> Status:
        return d.choice(self.sources).check(d)

    def evolve(self, d: Draw) -> Status:
        return self.avro.evolve(d) if self.avro is not None else Status.NOT_SENT

    def eventually(self, deadline: float) -> None:
        for source in self.sources:
            source.eventually(deadline)
