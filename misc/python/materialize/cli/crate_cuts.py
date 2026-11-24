#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Find crate splits and dependency edges that reduce crate connectivity.

Builds a module-level reference graph of the workspace from a SCIP index
produced by rust-analyzer, overlays it on the Cargo dependency graph, and
ranks two kinds of refactorings by how much they shrink the transitive
dependency closure of workspace crates:

* Crate splits. A crate is cut into a `core` part, the modules that do not
  reach a set of dependencies, and a `rest` part. Consumers that only use
  `core` modules stop depending on those dependencies.
* Edge cuts. A dependency edge whose references are few is a candidate for
  moving the referencing code or the referenced items.

Indexing needs `rust-analyzer` (`rustup component add rust-analyzer`) and
takes several minutes. The index is cached in the output directory, which
also receives `report.md`, `report.json`, the module graph `modules.json`, and
the crate graph `crates.dot`. Pass `--reindex` after changing code.

Proposals are candidates derived from references under the features Cargo
resolves for the workspace and `cfg(not(test))`. Code that only macro
expansions reference can be missed. Verify each proposal by performing the
split and running `cargo check`.
"""

import argparse
import json
import sys
from pathlib import Path

from materialize import MZ_ROOT, buildkite, ui
from materialize.crate_cuts.extract import extract, find_rust_analyzer, run_index
from materialize.crate_cuts.facts import Facts
from materialize.crate_cuts.model import Model
from materialize.crate_cuts.report import (
    crate_graph,
    modules_json,
    render,
    report_json,
)
from materialize.crate_cuts.search import analyze


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="crate-cuts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=MZ_ROOT / "target" / "crate-cuts",
        help="directory for the index and reports",
    )
    parser.add_argument("--index", type=Path, help="use an existing SCIP index")
    parser.add_argument(
        "--facts",
        type=Path,
        help="use an existing facts.json instead of indexing and extracting",
    )
    parser.add_argument("--reindex", action="store_true", help="rebuild the SCIP index")
    parser.add_argument("--rust-analyzer", help="rust-analyzer binary to use")
    parser.add_argument(
        "--rounds", type=int, default=10, help="greedy split rounds to plan"
    )
    parser.add_argument(
        "--objective",
        choices=["pairs", "lines"],
        default="pairs",
        help="rank by workspace pairs or by rebuild lines",
    )
    parser.add_argument(
        "--top", type=int, default=40, help="entries per report section"
    )
    parser.add_argument(
        "--annotate",
        action="store_true",
        help="post a summary as a Buildkite annotation",
    )
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.facts is not None:
        facts = Facts.from_json(json.loads(args.facts.read_text()))
    else:
        index = args.index or args.out_dir / "index.scip"
        if args.index is None and (args.reindex or not index.exists()):
            run_index(MZ_ROOT, args.rust_analyzer or find_rust_analyzer(), index)
        facts = extract(MZ_ROOT, index)
        (args.out_dir / "facts.json").write_text(json.dumps(facts.to_json()))

    model = Model(facts)
    graph = crate_graph(model)
    results = analyze(model, args.rounds, args.objective)

    (args.out_dir / "report.md").write_text(render(model, results, args.top))
    (args.out_dir / "report.json").write_text(
        json.dumps(report_json(model, results), indent=1)
    )
    (args.out_dir / "modules.json").write_text(
        json.dumps(modules_json(model), indent=1)
    )
    (args.out_dir / "crates.dot").write_text(graph)
    if args.annotate:
        baseline, final = results.baseline, results.final
        buildkite.add_annotation(
            "info",
            f"Crate cuts: {baseline.workspace_pairs} workspace pairs, "
            f"{len(results.applied)} planned splits save "
            f"{baseline.workspace_pairs - final.workspace_pairs}",
            render(model, results, 10),
            context="crate-cuts",
        )
    ui.say(
        f"wrote report.md, report.json, modules.json, and crates.dot to {args.out_dir}"
    )


if __name__ == "__main__":
    sys.exit(main())
