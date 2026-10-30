# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import copy
import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import bootstrap
from materialize.antithesis.drivers import rollouts

TARGET = "registry/environmentd:v26.49.0-dev.0--antithesis.gtarget"
BASE = "registry/environmentd:v26.48.2--antithesis.upgrade-base"
VERSION = "v26.49.0-dev.0 (target-build)"
OLD_VERSION = "v26.48.2 (base-build)"


class Kube:
    def __init__(self) -> None:
        self.obj: dict[str, Any] = {
            "metadata": {"generation": 1},
            "spec": {
                "environmentdImageRef": BASE,
                "requestRollout": "initial",
                "rolloutStrategy": "WaitUntilReady",
            },
            "status": {},
        }
        self.promote(BASE)
        self.patches = []
        self.outcome = "acked"
        self.ready = True
        self.started_at = 1
        self.leader = "IsLeader"
        self.direct_version = VERSION
        self.service_version = VERSION
        self.after_service = lambda: None
        self.before_snapshot = lambda: None

    def promote(self, image: str) -> None:
        self.actual_image = image
        self.pod_image = image
        self.generation = self.obj["metadata"]["generation"]
        self.pod_generation = self.generation
        self.obj["spec"]["environmentdImageRef"] = image
        self.obj["status"] = {
            "activeGeneration": self.generation,
            "resourceId": "test",
            "lastCompletedRolloutRequest": self.obj["spec"]["requestRollout"],
            "lastCompletedRolloutEnvironmentdImageRef": image,
            "conditions": [
                {
                    "type": "UpToDate",
                    "status": "True",
                    "reason": "Applied",
                    "observedGeneration": self.generation,
                }
            ],
        }

    def snapshot(self):
        self.before_snapshot()
        return rollouts.Snapshot(copy.deepcopy(self.obj))

    def patch_spec(self, changes):
        self.patches.append(dict(changes))
        outcome, self.outcome = self.outcome, "acked"
        if outcome == "not_landed":
            raise ApiException(status=504)
        if outcome == "interrupted":
            raise KeyboardInterrupt
        self.obj["spec"].update(changes)
        self.obj["metadata"]["generation"] += 1
        self.promote(self.obj["spec"]["environmentdImageRef"])
        if outcome == "landed":
            raise ApiException(status=504)
        return copy.deepcopy(self.obj)

    def envd_statefulsets(self, resource_id):
        return [
            SimpleNamespace(
                generation=self.generation, image=self.actual_image, deleting=False
            )
        ]

    def envd_pods(self):
        return [
            rollouts.EnvdPod(
                self.pod_generation,
                f"mztest-environmentd-{self.pod_generation}-0",
                "pod-uid",
                "pod-ip",
                0,
                self.started_at,
                self.ready,
                None,
                self.pod_image,
            )
        ]


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch, tmp_path):
    env = Mock(
        endpoints=SimpleNamespace(
            environmentd_image=TARGET, namespace="n", environment="e"
        )
    )
    env.sql_host.return_value = "service"
    clock = SimpleNamespace(now=0.0)
    api = Kube()
    path = tmp_path / "bootstrap.sqlite"
    monkeypatch.setattr(bootstrap, "Environment", lambda: env)
    monkeypatch.setattr(bootstrap.state, "open_db", lambda *args: sqlite3.connect(path))
    monkeypatch.setattr(rollouts, "Kube", lambda _: api)
    monkeypatch.setattr(
        rollouts, "leader_statuses", lambda pods: {pods[0].uid: api.leader}
    )
    monkeypatch.setattr(bootstrap.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(
        bootstrap.time,
        "sleep",
        lambda seconds: setattr(clock, "now", clock.now + seconds),
    )
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_SECONDS", 3.0)
    sql_hosts = []

    @contextmanager
    def connection(host, **kwargs):
        sql_hosts.append(host)
        assert kwargs == {"connect_timeout": 5, "statement_timeout_ms": 5_000}
        conn = Mock()
        version = api.direct_version if host == "pod-ip" else api.service_version
        conn.execute.return_value.fetchone.return_value = (version,)
        yield conn
        if host == "service":
            api.after_service()

    monkeypatch.setattr(bootstrap.sql, "connection", connection)
    return SimpleNamespace(
        env=env, api=api, clock=clock, path=path, sql_hosts=sql_hosts
    )


@pytest.mark.parametrize("outcome", ["acked", "landed", "not_landed"])
def test_upgrade_reuses_the_request_after_lost_responses(harness, outcome):
    harness.api.outcome = outcome
    bootstrap.ensure_target_image()
    assert len(harness.api.patches) == (2 if outcome == "not_landed" else 1)
    requests = {patch["requestRollout"] for patch in harness.api.patches}
    assert len(requests) == 1
    assert all(patch["environmentdImageRef"] == TARGET for patch in harness.api.patches)
    assert harness.sql_hosts == ["pod-ip", "service"]
    with sqlite3.connect(harness.path) as db:
        request, deadline, completed = db.execute(
            "SELECT request, deadline, completed FROM target_image"
        ).fetchone()
    assert requests == {request}
    assert deadline == 3.0 and completed == 1


def test_interrupted_bootstrap_keeps_request_and_deadline(harness):
    harness.api.outcome = "interrupted"
    with pytest.raises(KeyboardInterrupt):
        bootstrap.ensure_target_image()
    harness.clock.now = 1.0
    bootstrap.ensure_target_image()
    assert (
        harness.api.patches[0]["requestRollout"]
        == harness.api.patches[1]["requestRollout"]
    )
    with sqlite3.connect(harness.path) as db:
        assert db.execute("SELECT deadline FROM target_image").fetchone() == (3.0,)


def test_an_interrupted_bootstrap_cannot_restart_its_timeout_budget(harness):
    harness.api.outcome = "interrupted"
    with pytest.raises(KeyboardInterrupt):
        bootstrap.ensure_target_image()
    harness.clock.now = 4.0
    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()
    assert len(harness.api.patches) == 1


@pytest.mark.parametrize(
    "target", [TARGET, "environmentd:local", "registry/environmentd@sha256:" + "a" * 64]
)
def test_already_current_targets_include_aliases_and_digests(harness, target):
    harness.env.endpoints.environmentd_image = target
    harness.api.promote(target)
    bootstrap.ensure_target_image()
    assert not harness.api.patches
    assert harness.sql_hosts == ["pod-ip", "service"]


@pytest.mark.parametrize(
    "versions", [(OLD_VERSION, OLD_VERSION), (VERSION, OLD_VERSION)]
)
def test_target_status_cannot_hide_an_old_sql_server(harness, versions):
    harness.api.promote(TARGET)
    harness.api.direct_version, harness.api.service_version = versions
    with pytest.raises(TimeoutError) as error:
        bootstrap.ensure_target_image()
    assert TARGET in str(error.value)
    assert "last_status" in str(error.value) and "serving_versions" in str(error.value)
    assert not harness.api.patches


@pytest.mark.parametrize(
    "state",
    ["unobserved", "wrong_image", "stale_pod", "not_running", "unready", "not_leader"],
)
def test_current_image_still_requires_a_current_ready_active_pod(harness, state):
    harness.api.promote(TARGET)
    if state == "unobserved":
        harness.api.obj["status"]["conditions"][0]["observedGeneration"] = 0
    elif state == "wrong_image":
        harness.api.pod_image = BASE
    elif state == "stale_pod":
        harness.api.pod_generation -= 1
    elif state == "not_running":
        harness.api.started_at = None
    elif state == "unready":
        harness.api.ready = False
    else:
        harness.api.leader = "Initializing"
    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()
    assert not harness.sql_hosts


def test_bootstrap_rechecks_the_active_generation_after_sql(harness):
    harness.api.promote(TARGET)
    harness.api.after_service = lambda: harness.api.obj["status"].update(
        activeGeneration=20
    )
    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()


@pytest.mark.parametrize("stale", [False, True])
def test_bootstrap_promotes_only_a_current_ready_request(harness, stale):
    harness.api.promote(TARGET)
    condition = harness.api.obj["status"]["conditions"][0]
    condition.update(reason="ReadyToPromote", status="False")
    if stale:
        condition["observedGeneration"] = 0
        with pytest.raises(TimeoutError):
            bootstrap.ensure_target_image()
        assert not harness.api.patches
    else:
        bootstrap.ensure_target_image()
        assert harness.api.patches == [{"forcePromote": "initial"}]


@pytest.mark.parametrize("healthy", [False, True])
def test_completed_journal_is_revalidated_after_its_deadline(harness, healthy):
    harness.api.promote(TARGET)
    bootstrap.ensure_target_image()
    harness.clock.now = 10.0
    if healthy:
        bootstrap.ensure_target_image()
        assert harness.sql_hosts == ["pod-ip", "service", "pod-ip", "service"]
    else:
        harness.api.actual_image = BASE
        with pytest.raises(TimeoutError):
            bootstrap.ensure_target_image()


def test_target_observed_only_after_the_deadline_does_not_pass(harness):
    harness.api.promote(TARGET)
    harness.api.before_snapshot = lambda: setattr(harness.clock, "now", 4.0)
    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()
    with sqlite3.connect(harness.path) as db:
        assert db.execute("SELECT completed FROM target_image").fetchone() == (0,)


def test_pod_image_identity_uses_the_spec_reference_after_status_normalization():
    kube = object.__new__(rollouts.Kube)
    kube.namespace = "n"
    kube.core = Mock()
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="mztest-environmentd-1-0",
            uid="u",
            annotations={"materialize.cloud/generation": "1"},
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(name="environmentd", image="environmentd:local")
            ]
        ),
        status=SimpleNamespace(
            pod_ip="pod-ip",
            container_statuses=[
                SimpleNamespace(
                    name="environmentd",
                    image="docker.io/library/environmentd:local",
                    ready=True,
                    restart_count=0,
                    state=SimpleNamespace(
                        running=SimpleNamespace(
                            started_at=SimpleNamespace(timestamp=lambda: 1)
                        )
                    ),
                    last_state=None,
                )
            ],
        ),
    )
    kube.core.list_namespaced_pod.return_value.items = [pod]
    observed = kube.envd_pods()
    assert len(observed) == 1
    assert observed[0].image == "environmentd:local"
    assert observed[0].ready and observed[0].started_at == 1


@pytest.mark.parametrize("deleting", [False, True])
def test_active_target_is_not_blocked_by_old_generation_lists(
    harness, monkeypatch: pytest.MonkeyPatch, deleting
):
    harness.api.promote(TARGET)
    current_sets = harness.api.envd_statefulsets("test")
    old_set = SimpleNamespace(generation=0, image=BASE, deleting=deleting)
    monkeypatch.setattr(
        harness.api, "envd_statefulsets", lambda _: [old_set, *current_sets]
    )
    current_pods = harness.api.envd_pods()
    old_pod = rollouts.EnvdPod(
        0, "mztest-environmentd-0-0", "old-uid", None, 0, None, False, None, BASE
    )
    monkeypatch.setattr(harness.api, "envd_pods", lambda: [old_pod, *current_pods])

    bootstrap.ensure_target_image()

    assert harness.sql_hosts == ["pod-ip", "service"]
    assert not harness.api.patches


def test_bootstrap_persists_waiting_state_without_repeating_unchanged_logs(
    harness, capsys
):
    harness.api.promote(TARGET)
    harness.api.ready = False
    harness.api.before_snapshot = lambda: harness.api.obj["metadata"].update(
        resourceVersion=str(harness.clock.now)
    )

    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()

    with sqlite3.connect(harness.path) as db:
        details = json.loads(
            db.execute("SELECT details FROM bootstrap_progress").fetchone()[0]
        )
    assert details["waiting_for"] == "active_pod"
    assert details["pod"]["ready"] is False
    assert capsys.readouterr().out.count("focused bootstrap:") == 1


def test_bootstrap_records_verified_readiness_before_returning(harness, capsys):
    harness.api.promote(TARGET)

    bootstrap.ensure_target_image()

    with sqlite3.connect(harness.path) as db:
        details = json.loads(
            db.execute("SELECT details FROM bootstrap_progress").fetchone()[0]
        )
    assert details["waiting_for"] == "ready"
    assert details["serving_versions"] == [VERSION, VERSION]
    assert capsys.readouterr().out.count("focused bootstrap:") == 1


def test_initial_upgrade_skips_empty_catalog_catchup(harness):
    bootstrap.ensure_target_image()

    patch = harness.api.patches[0]
    assert patch["forcePromote"] == patch["requestRollout"]


def test_interrupted_own_applying_request_is_force_promoted(harness):
    harness.api.outcome = "interrupted"
    with pytest.raises(KeyboardInterrupt):
        bootstrap.ensure_target_image()
    request = harness.api.patches[0]["requestRollout"]
    harness.api.obj["spec"].update(
        environmentdImageRef=TARGET,
        requestRollout=request,
        forceRollout=request,
        forcePromote=None,
    )
    harness.api.obj["metadata"]["generation"] += 1
    harness.api.obj["status"]["conditions"][0].update(
        status="Unknown",
        reason="Applying",
        observedGeneration=harness.api.obj["metadata"]["generation"],
    )

    bootstrap.ensure_target_image()

    assert harness.api.patches[-1] == {"forcePromote": request}
    assert harness.sql_hosts == ["pod-ip", "service"]


def test_bootstrap_does_not_force_another_applying_request(harness):
    harness.api.promote(TARGET)
    harness.api.obj["status"]["conditions"][0].update(
        status="Unknown", reason="Applying"
    )

    with pytest.raises(TimeoutError):
        bootstrap.ensure_target_image()

    assert not harness.api.patches
