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

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.materialize.impl import MaterializeAdapter
from jinja2 import Environment

MACROS = Path(__file__).parents[2] / "dbt/include/materialize/macros"


class MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


def macro_return(value):
    raise MacroReturn(value)


def quote(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def deployment(
    managed, policy_rows, catalog_exists=True, query_error=None, creation_error=None
):
    statements = []

    def run_query(sql):
        statements.append(sql)
        if "CREATE CLUSTER" in sql and creation_error:
            raise creation_error
        if "mz_catalog.mz_objects" in sql:
            rows = [(catalog_exists,)]
        elif "mz_internal.mz_query_policy_attachments" in sql:
            if query_error:
                raise query_error
            rows = policy_rows
        elif "c.managed" in sql:
            rows = [(managed, "25cc", 1, "manual", None)]
        elif "mz_cluster_replicas cr" in sql:
            rows = [("Replica mixed case", "25cc", None)]
        else:
            rows = []
        return SimpleNamespace(rows=rows)

    def compiler_error(message):
        raise ValueError(message)

    context = dict(
        execute=True,
        target=SimpleNamespace(name="default"),
        adapter=SimpleNamespace(
            quote=MaterializeAdapter.quote,
            generate_final_cluster_name=lambda name, force_deploy_suffix=False: (
                name + "_dbt_deploy" if force_deploy_suffix else name
            ),
        ),
        dbt=SimpleNamespace(string_literal=literal),
        exceptions=SimpleNamespace(raise_compiler_error=compiler_error),
        return_=macro_return,
        run_query=run_query,
        log=Mock(return_value=""),
        internal_get_deployment_config=lambda: {"clusters": ["Prod mixed case"]},
        cluster_exists=lambda name: not name.endswith("_dbt_deploy"),
        cluster_contains_sinks=lambda name: False,
        deploy_validate_permissions=Mock(return_value=""),
        internal_get_cluster_auto_scaling_strategy=lambda name: None,
        set_cluster_ci_tag=Mock(return_value=""),
    )
    context["return"] = context.pop("return_")
    env = Environment(extensions=["jinja2.ext.do"])

    def load(path, name):
        template = env.from_string((MACROS / path).read_text())

        def call(*args, **kwargs):
            module = template.make_module(context)
            try:
                return getattr(module, name)(*args, **kwargs)
            except MacroReturn as result:
                return result.value

        context[name] = call

    load("utils/query_policy.sql", "internal_get_cluster_query_policy")
    load("ci/create_cluster.sql", "create_cluster")
    load("ci/create_unmanaged_cluster.sql", "create_unmanaged_cluster")
    load("deploy/deploy_init.sql", "deploy_init")
    return context["deploy_init"], statements


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("policy", [None, "policy", 'Policy "quoted"'])
def test_deploy_preserves_query_policy(managed, policy):
    deploy, statements = deployment(managed, [] if policy is None else [(policy,)])
    deploy()
    creates = [s for s in statements if "CREATE CLUSTER" in s]
    assert len(creates) == 1
    assert quote("Prod mixed case_dbt_deploy") in creates[0]
    if policy is None:
        assert "QUERY POLICY" not in creates[0]
    else:
        assert "QUERY POLICY = " + quote(policy) in creates[0]
    if not managed:
        assert quote("Replica mixed case") in creates[0]


@pytest.mark.parametrize("managed", [False, True])
def test_deploy_before_query_policy_catalog(managed):
    deploy, statements = deployment(managed, [], catalog_exists=False)
    deploy()
    assert not any(
        "FROM mz_internal.mz_query_policy_attachments" in s for s in statements
    )
    assert any("CREATE CLUSTER" in s for s in statements)
    assert not any("QUERY POLICY" in s for s in statements)


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("failure", ["permission", "unresolved"])
def test_deploy_policy_lookup_fails_closed(managed, failure):
    deploy, statements = deployment(
        managed,
        [(None,)],
        query_error=(
            PermissionError("permission denied") if failure == "permission" else None
        ),
    )
    with pytest.raises((PermissionError, ValueError)):
        deploy()
    assert not any("CREATE CLUSTER" in s for s in statements)


@pytest.mark.parametrize("managed", [False, True])
def test_deploy_policy_attachment_fails_closed(managed):
    deploy, statements = deployment(
        managed,
        [("policy",)],
        creation_error=PermissionError("permission denied for query policy"),
    )
    with pytest.raises(PermissionError, match="permission denied for query policy"):
        deploy()
    creates = [s for s in statements if "CREATE CLUSTER" in s]
    assert len(creates) == 1
    assert 'QUERY POLICY = "policy"' in creates[0]
