# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class EncodeHashFunctions(Check):
    """encode/decode and the hash function family."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE encode_fns_table (id INT, txt STRING)
            > INSERT INTO encode_fns_table VALUES (1, 'abc')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO encode_fns_table VALUES (2, 'hello world')

                > CREATE MATERIALIZED VIEW encode_fns_encode1 AS
                  SELECT
                    id,
                    encode(txt::bytea, 'base64') AS b64,
                    encode(txt::bytea, 'hex') AS hex,
                    encode(txt::bytea, 'escape') AS esc,
                    convert_from(decode(encode(txt::bytea, 'base64'), 'base64'), 'utf8') AS roundtrip
                  FROM encode_fns_table
                """,
                """
                > INSERT INTO encode_fns_table VALUES (3, 'Matérialize')

                > CREATE MATERIALIZED VIEW encode_fns_hash1 AS
                  SELECT
                    id,
                    md5(txt) AS md5_hash,
                    encode(sha256(txt::bytea), 'hex') AS sha256_hash,
                    encode(digest(txt, 'sha1'), 'hex') AS digest_hash,
                    encode(hmac(txt, 'key', 'sha256'), 'hex') AS hmac_hash,
                    crc32(txt) AS crc,
                    kafka_murmur2(txt) AS murmur,
                    seahash(txt) AS sea
                  FROM encode_fns_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM encode_fns_encode1
            1 YWJj 616263 abc abc
            2 "aGVsbG8gd29ybGQ=" "68656c6c6f20776f726c64" "hello world" "hello world"
            3 TWF0w6lyaWFsaXpl 4d6174c3a97269616c697a65 Mat\\303\\251rialize Matérialize

            > SELECT id, md5_hash, sha256_hash FROM encode_fns_hash1 WHERE id = 1
            1 900150983cd24fb0d6963f7d28e17f72 ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad

            > SELECT count(*) FROM encode_fns_hash1 WHERE digest_hash IS NULL OR hmac_hash IS NULL OR crc IS NULL OR murmur IS NULL OR sea IS NULL
            0

            # The hashes must be stable across restarts and versions: the
            # maintained values must equal freshly computed ones.
            > SELECT count(*)
              FROM encode_fns_hash1 h JOIN encode_fns_table t ON h.id = t.id
              WHERE h.md5_hash != md5(t.txt)
                 OR h.sha256_hash != encode(sha256(t.txt::bytea), 'hex')
                 OR h.digest_hash != encode(digest(t.txt, 'sha1'), 'hex')
                 OR h.hmac_hash != encode(hmac(t.txt, 'key', 'sha256'), 'hex')
                 OR h.crc != crc32(t.txt)
                 OR h.murmur != kafka_murmur2(t.txt)
                 OR h.sea != seahash(t.txt)
            0
            """))


class NormalizeFunction(Check):
    """normalize (Unicode normalization), which only exists from v0.157."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.157.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE normalize_fns_table (id INT, txt STRING)
            > INSERT INTO normalize_fns_table VALUES (1, 'abc')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO normalize_fns_table VALUES (2, 'hello world')
                """,
                """
                > INSERT INTO normalize_fns_table VALUES (3, 'Matérialize')

                > CREATE MATERIALIZED VIEW normalize_fns_mv1 AS
                  SELECT
                    id,
                    octet_length(normalize(txt, NFC)) AS nfc_len,
                    octet_length(normalize(txt, NFD)) AS nfd_len,
                    octet_length(normalize(txt, NFKC)) AS nfkc_len,
                    octet_length(normalize(txt, NFKD)) AS nfkd_len,
                    length(txt) AS chars,
                    octet_length(txt) AS octets,
                    bit_length(txt) AS bits
                  FROM normalize_fns_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM normalize_fns_mv1
            1 3 3 3 3 3 3 24
            2 11 11 11 11 11 11 88
            3 12 13 12 13 11 12 96
            """))


class MathFunctions(Check):
    """Math and trigonometry functions from the functions index."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE math_fns_table (id INT, x DOUBLE PRECISION, n NUMERIC)
            > INSERT INTO math_fns_table VALUES (1, 4.0, 2.5)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO math_fns_table VALUES (2, 0.0, -3.7)

                > CREATE MATERIALIZED VIEW math_fns_mv1 AS
                  SELECT
                    id,
                    abs(n) AS abs_n,
                    round(n, 1) AS round_n,
                    floor(n) AS floor_n,
                    ceil(n) AS ceil_n,
                    trunc(n) AS trunc_n,
                    sqrt(x) AS sqrt_x,
                    cbrt(x * 2) AS cbrt_x,
                    power(x, 2) AS pow_x,
                    mod(8, 3) AS mod_c
                  FROM math_fns_table
                """,
                """
                > INSERT INTO math_fns_table VALUES (3, 1.0, 10)

                > CREATE MATERIALIZED VIEW math_fns_mv2 AS
                  SELECT
                    id,
                    ln(1.0::double) AS ln_c,
                    exp(0.0::double) AS exp_c,
                    log10(100.0::double) AS log_c,
                    sin(0.0::double) AS sin_c,
                    cos(0.0::double) AS cos_c,
                    round(degrees(radians(180.0))::numeric, 4) AS deg,
                    round(radians(180.0)::numeric, 4) AS rad
                  FROM math_fns_table
                  WHERE x > 0
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM math_fns_mv1
            1 2.5 2.5 2 3 2 2 2 16 2
            2 3.7 -3.7 -4 -3 -3 0 0 0 2
            3 10 10 10 10 10 1 1.2599210498948732 1 2

            > SELECT * FROM math_fns_mv2
            1 0 1 2 0 1 180 3.1416
            3 0 1 2 0 1 180 3.1416
            """))
