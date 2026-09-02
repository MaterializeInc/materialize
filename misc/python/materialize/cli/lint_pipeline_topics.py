# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# lint_pipeline_topics.py - Check that the `topics` fields in CI pipeline
# templates match the services declared by each step's mzcompose composition

import argparse
import importlib.abc
import importlib.util
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT, mzbuild
from materialize.mzcompose import loader
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.polaris import Polaris
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost

TEMPLATES = [
    "ci/nightly/pipeline.template.yml",
    "ci/test/pipeline.template.yml",
    "ci/release-qualification/pipeline.template.yml",
]

# Topics derivable from a composition's SERVICES list. A step running an
# mzcompose composition must declare exactly the subset of these that its
# composition's services map to.
SERVICE_TOPICS: list[tuple[type, str]] = [
    (MySql, "mysql"),
    (Postgres, "postgres"),
    (Kafka, "kafka"),
    # Redpanda stands in for Kafka, tests using it exercise the same code.
    (Redpanda, "kafka"),
    (SqlServer, "sql-server"),
    (Cockroach, "cockroach"),
    (Polaris, "iceberg"),
    (Debezium, "debezium"),
    (SshBastionHost, "ssh-tunnel"),
]

AUTO_TOPICS = {topic for _, topic in SERVICE_TOPICS}

# Topics derivable from a step's mzcompose plugin arguments. Azurite sits in
# the SERVICES list of many compositions but is only exercised by the steps
# that opt in via --azurite or --features=azurite.
ARGS_TOPICS = {"azurite"}

# Topics assigned by judgment of what a step exercises rather than which
# services it starts. The lint can only vet them against this vocabulary.
MANUAL_TOPICS = {"upgrade", "0dt-migration", "kafka-sink", "copy-to-s3", "self-managed"}

# Steps that touch a topic through infrastructure the service derivation
# cannot see. Keyed by step id, values are the additional auto topics the
# step may declare beyond what its composition's services imply.
EXTRA_STEP_TOPICS = {
    # Iceberg sinks against the real AWS/GCP catalogs, no local Polaris.
    "aws-real": {"iceberg"},
    "gcp-real": {"iceberg"},
}


def pipeline_steps(pipeline: Any) -> Any:
    for step in pipeline["steps"]:
        yield step
        if "group" in step:
            yield from step.get("steps", [])


def step_composition(step: dict[str, Any]) -> str | None:
    for plugin in step.get("plugins", []):
        if not isinstance(plugin, dict):
            continue
        for plugin_name, plugin_config in plugin.items():
            if plugin_name == "./ci/plugins/mzcompose":
                return plugin_config["composition"]
    return None


def step_args_topics(step: dict[str, Any]) -> set[str]:
    """Derive topics from a step's mzcompose plugin arguments."""
    for plugin in step.get("plugins", []):
        if not isinstance(plugin, dict):
            continue
        for plugin_name, plugin_config in plugin.items():
            if plugin_name == "./ci/plugins/mzcompose":
                if any(
                    "azurite" in str(arg) for arg in plugin_config.get("args") or []
                ):
                    return {"azurite"}
    return set()


def derive_topics(composition_path: Path) -> set[str]:
    """Derive the auto topics of a composition from its SERVICES list."""
    spec = importlib.util.spec_from_file_location(
        "mzcompose", composition_path / "mzcompose.py"
    )
    assert spec
    module = importlib.util.module_from_spec(spec)
    assert isinstance(spec.loader, importlib.abc.Loader)
    loader.composition_path = composition_path
    try:
        spec.loader.exec_module(module)
    finally:
        loader.composition_path = None

    topics = set()
    for service in getattr(module, "SERVICES", []):
        # PostgresMetadata backs the metadata store of nearly every
        # composition, which says nothing about the test exercising Postgres
        # as a source.
        if isinstance(service, PostgresMetadata):
            continue
        for cls, topic in SERVICE_TOPICS:
            if isinstance(service, cls):
                topics.add(topic)
    return topics


def _derive_named(arg: tuple[str, Path]) -> tuple[str, set[str]]:
    name, path = arg
    return name, derive_topics(path)


def check_template(template: str, derived_topics: dict[str, set[str]]) -> list[str]:
    errors = []
    with open(MZ_ROOT / template) as f:
        pipeline = yaml.safe_load(f)

    for step in pipeline_steps(pipeline):
        ident = step.get("id") or step.get("command") or step.get("group")
        declared = step.get("topics", [])
        if not isinstance(declared, list) or not all(
            isinstance(topic, str) for topic in declared
        ):
            errors.append(
                f"{template}: step {ident!r}: `topics` must be a list of strings"
            )
            continue
        declared_set = set(declared)
        if len(declared_set) != len(declared):
            errors.append(f"{template}: step {ident!r}: duplicate entries in `topics`")
        if unknown := declared_set - AUTO_TOPICS - ARGS_TOPICS - MANUAL_TOPICS:
            errors.append(
                f"{template}: step {ident!r}: unknown topics {sorted(unknown)}, "
                f"known are {sorted(AUTO_TOPICS | ARGS_TOPICS)} (derived from "
                f"services/args) and {sorted(MANUAL_TOPICS)} (assigned by judgment)"
            )

        composition = step_composition(step)
        if composition is None:
            # Steps without a composition (cloudtest, trigger steps) have
            # nothing to derive topics from, all their topics are judgment.
            continue

        expected = (
            derived_topics[composition]
            | step_args_topics(step)
            | EXTRA_STEP_TOPICS.get(step.get("id"), set())
        )
        if declared_set & (AUTO_TOPICS | ARGS_TOPICS) != expected:
            want = sorted(expected | (declared_set & MANUAL_TOPICS))
            errors.append(
                f"{template}: step {ident!r}: composition {composition!r} requires "
                f"`topics: [{', '.join(want)}]`, found "
                f"`topics: [{', '.join(sorted(declared_set))}]`"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="lint-pipeline-topics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Check that the `topics` fields in CI pipeline templates match the services declared by each step's mzcompose composition",
    )
    parser.parse_args()

    repo = mzbuild.Repository(Path("."))

    # Collect the compositions used across all templates, then derive each
    # composition's topics only once.
    compositions = set()
    for template in TEMPLATES:
        with open(MZ_ROOT / template) as f:
            pipeline = yaml.safe_load(f)
        for step in pipeline_steps(pipeline):
            if composition := step_composition(step):
                compositions.add(composition)

    # Importing each composition's mzcompose.py takes a moment, spread the
    # work across processes.
    with ProcessPoolExecutor() as pool:
        derived_topics = dict(
            pool.map(
                _derive_named,
                [(c, repo.compositions[c]) for c in sorted(compositions)],
            )
        )

    errors = []
    for template in TEMPLATES:
        errors.extend(check_template(template, derived_topics))

    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
