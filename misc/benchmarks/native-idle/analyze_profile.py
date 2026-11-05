# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import collections
import json
import re
import subprocess
import sys
from pathlib import Path

p = Path(sys.argv[1])
role = sys.argv[2]
kind = sys.argv[3] if len(sys.argv) > 3 else "cpu"
assert kind in {"cpu", "heap"}
prefix = role + "-" + kind
raw = subprocess.check_output(
    ["go", "tool", "pprof", "-raw", "-symbolize=none", str(p / (prefix + ".pb.gz"))],
    text=True,
)
(p / (prefix + "-raw.txt")).write_text(raw)
binary = (p / (role + "-binary.txt")).read_text().strip()
notes = subprocess.check_output(["readelf", "-n", binary], text=True)
build_id = re.search(r"Build ID: ([0-9a-f]+)", notes).group(1)
maps = {
    l.split()[0].rstrip(":")
    for l in raw.split("Mappings\n")[1].splitlines()
    if len(l.split()) >= 4 and l.split()[3] == build_id
}
assert maps, "profile must map the exact symbol binary build ID"
locations = [
    (int(m[1]), m[2])
    for m in re.finditer(r"^\s+(\d+): (0x[0-9a-f]+) M=(\d+)", raw, re.M)
    if m[3] in maps
]
symbolized = subprocess.check_output(
    ["llvm-symbolizer", "--no-inlines", "--demangle", "--obj=" + binary],
    input="\n".join(a for _, a in locations) + "\n",
    text=True,
)
(p / (prefix + "-all-symbols.txt")).write_text(symbolized)
symbols = {
    i: b.splitlines()[0]
    for (i, _), b in zip(locations, symbolized.strip().split("\n\n"))
}
sample_header = raw.split("Samples:", 1)[1].strip().splitlines()[0].split()
selected = next(
    (
        i
        for i, v in enumerate(sample_header)
        if ("bytes" if kind == "heap" else "count") in v
    ),
    None,
)
assert selected is not None, sample_header
counts = collections.Counter()
total = 0
for l in raw.split("Locations")[0].splitlines():
    m = re.match(r"\s+([0-9 ]+): ([0-9 ]+)\s*$", l)
    if m:
        values = [int(v) for v in m[1].split()]
        assert len(values) == len(sample_header), (values, sample_header)
        weight = values[selected]
        total += weight
        counts.update({symbols.get(int(i), "?"): weight for i in m[2].split()})
assert total > 0
result = {
    "sample_type": sample_header[selected],
    "kind": kind,
    "build_id": build_id,
    "total_weight": total,
    "inclusive_samples": counts.most_common(),
}
(p / (prefix + "-analysis.json")).write_text(json.dumps(result, indent=2) + "\n")
print(role, kind, sample_header[selected], total)
for f, c in counts.most_common():
    if f.startswith(("mz_", "<mz_")):
        print(round(100 * c / total, 1), f[:180])
    if c < total * 0.09:
        break
