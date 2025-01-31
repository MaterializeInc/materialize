# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# helm_chart_version_bump — Bump environmentd/orchestratord versions in helm chart

import argparse

from ruamel.yaml import YAML

from materialize import MZ_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="helm-chart-version-bump",
        description="Bump environmentd/orchestratord versions in helm chart.",
    )
    parser.add_argument(
        "--helm-chart-version",
        type=str,
        help="Helm-chart version to bump to, no change if not set.",
    )
    parser.add_argument("version", type=str, help="Materialize version to bump to.")
    args = parser.parse_args()

    yaml = YAML()
    yaml.preserve_quotes = True

    # TODO: The version is currently spread across many yaml files, is that
    # necessary? Would be nicer to only bump in one place
    mods = [
        (
            MZ_ROOT / "misc" / "helm-charts" / "operator" / "values.yaml",
            lambda docs: docs[0]["operator"]["image"].update({"tag": args.version}),
        ),
        (
            MZ_ROOT / "misc" / "helm-charts" / "operator" / "Chart.yaml",
            lambda docs: docs[0].update({"appVersion": args.version}),
        ),
        (
            MZ_ROOT / "misc" / "helm-charts" / "testing" / "materialize.yaml",
            lambda docs: docs[2]["spec"].update(
                {"environmentdImageRef": f"materialize/environmentd:{args.version}"}
            ),
        ),
        (
            MZ_ROOT
            / "misc"
            / "helm-charts"
            / "operator"
            / "tests"
            / "deployment_test.yaml",
            lambda docs: docs[0]["tests"][0]["asserts"][1]["equal"].update(
                {"value": f"materialize/orchestratord:{args.version}"}
            ),
        ),
    ]

    if args.helm_chart_version:
        mods.append(
            (
                MZ_ROOT / "misc" / "helm-charts" / "operator" / "Chart.yaml",
                lambda docs: docs[0].update({"version": args.helm_chart_version}),
            )
        )

    for file, mod in mods:
        with open(file) as f:
            docs = list(yaml.load_all(f))
        mod(docs)
        with open(file, "w") as f:
            yaml.dump_all(docs, f)

    return 0


if __name__ == "__main__":
    main()
