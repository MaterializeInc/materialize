# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cargo_bench.host import cpu_identity

X86_CPUINFO = """\
processor\t: 0
vendor_id\t: AuthenticAMD
model name\t: AMD EPYC-Milan Processor
microcode\t: 0x1000065
flags\t\t: fpu vme

processor\t: 1
vendor_id\t: AuthenticAMD
model name\t: AMD EPYC-Genoa Processor
microcode\t: 0xa101148
"""

AARCH64_CPUINFO = """\
processor\t: 0
BogoMIPS\t: 50.00
CPU implementer\t: 0x41
CPU part\t: 0xd0c
"""


def test_cpu_identity_reads_first_processor() -> None:
    assert cpu_identity(X86_CPUINFO) == ("AMD EPYC-Milan Processor", "0x1000065")


def test_cpu_identity_without_model_or_microcode() -> None:
    assert cpu_identity(AARCH64_CPUINFO) == (None, None)
    assert cpu_identity("") == (None, None)
