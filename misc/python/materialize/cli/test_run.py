# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import signal
import socket as socket_module
import subprocess
import sys
import time
import uuid
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import psutil
import pytest

from materialize.cli import run
from materialize.ui import UIError


@pytest.fixture
def launcher(monkeypatch, tmp_path):
    monkeypatch.setattr(run, "MZ_ROOT", tmp_path)
    monkeypatch.setattr(run, "MZDATA", tmp_path / "mzdata")
    for key in ("MZDEV_POSTGRES", "MZDEV_BLOB"):
        monkeypatch.delenv(key, raising=False)
    build = MagicMock(
        return_value=(
            0,
            [
                str(tmp_path / "target" / "optimized" / name)
                for name in ("clusterd", "adapterd", "pubsubd", "controllerd")
            ],
        )
    )
    monkeypatch.setattr(run, "_cargo_build", build)
    version = MagicMock(return_value="adapterd v1\nwritten-plan-build: 1.2.3+build\n")
    monkeypatch.setattr(run.subprocess, "check_output", version)
    connect = MagicMock()
    connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        None
    )
    monkeypatch.setattr(run, "_connect_sql", connect)
    sql = MagicMock()
    monkeypatch.setattr(run, "_run_sql", sql)
    socket = MagicMock()
    monkeypatch.setattr(socket_module, "create_connection", socket)
    launch = MagicMock(return_value=0)
    monkeypatch.setattr(run, "_run_controller", launch)
    monkeypatch.setattr(
        run,
        "_handle_lingering_services",
        MagicMock(side_effect=AssertionError("must not scan/kill children")),
    )
    monkeypatch.setattr(
        run.os,
        "fork",
        MagicMock(side_effect=AssertionError("must not use group cleanup")),
    )
    monkeypatch.setattr(run.sys, "argv", ["run", "controllerd", "--optimized"])
    return build, version, connect, sql, socket, launch


@pytest.mark.parametrize("optimized", [False, True])
def test_controller_launch_and_restart(launcher, tmp_path, monkeypatch, optimized):
    monkeypatch.setattr(
        run.sys, "argv", ["run", "controllerd"] + (["--optimized"] if optimized else [])
    )
    build, version, connect, sql, socket, launch = launcher
    assert run.main() == 0
    build.assert_called_once()
    assert build.call_args.kwargs["extra_programs"] == [
        "adapterd",
        "pubsubd",
        "controllerd",
    ]
    assert build.call_args.args[0].optimized == optimized
    connect.assert_called_once_with(
        "postgres://root@localhost:26257/materialize_controllerd"
    )
    version.assert_called_once_with(
        [str(tmp_path / "mzbuild/bin/adapterd"), "--version"], text=True
    )
    socket.assert_not_called()
    command = launch.call_args.args[0]
    assert command[0] == str(tmp_path / "mzbuild/bin/controllerd")
    assert "--persist-pubsub-external" not in command
    assert f"--pubsubd-image={tmp_path}/mzbuild/bin/pubsubd" in command
    if not optimized:
        assert launch.call_args.args[1]["RUST_MIN_STACK"] == run.RUST_MIN_STACK
    assert "--adapter-plan-build=1.2.3+build" in command
    assert f"--adapterd-image={tmp_path}/mzbuild/bin/adapterd" in command
    assert (
        f"--persist-blob-url=file://{tmp_path}/mzdata-controllerd/persist/blob"
        in command
    )
    assert '--adapter-sizes={"1":{"cpu_limit":1,"memory_limit":"4 GiB"}}' in command
    assert [call.args[1] for call in sql.call_args_list] == [
        f"CREATE SCHEMA IF NOT EXISTS {schema}"
        for schema in ("consensus", "tsoracle", "storage")
    ]
    identity = (tmp_path / "mzdata-controllerd/environment-id").read_text()
    # A restart does not inspect or recreate adapter declarations, even if none
    # remain. Only the controller decides whether to initialize the catalog.
    assert run.main() == 0
    assert launch.call_args.args[0] == command
    assert (tmp_path / "mzdata-controllerd/environment-id").read_text() == identity
    assert not (tmp_path / "mzdata").exists()


def test_controller_reset_rejected_before_build(launcher, monkeypatch):
    monkeypatch.setattr(run.sys, "argv", ["run", "controllerd", "--reset"])
    with pytest.raises(UIError, match="does not support --reset"):
        run.main()
    launcher[0].assert_not_called()
    launcher[2].assert_not_called()


def test_controller_build_only_needs_no_services(launcher, monkeypatch):
    monkeypatch.setattr(
        run.sys, "argv", ["run", "controllerd", "--optimized", "--build-only"]
    )
    assert run.main() == 0
    for mock in launcher[1:]:
        mock.assert_not_called()


def test_controller_refuses_existing_metadata(launcher):
    launcher[
        2
    ].return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "consensus",
    )
    with pytest.raises(UIError, match="existing environments cannot be converted"):
        run.main()
    launcher[3].assert_not_called()
    launcher[5].assert_not_called()


def test_controller_refuses_environmentd_state(launcher, monkeypatch, tmp_path):
    monkeypatch.setattr(
        run.sys,
        "argv",
        ["run", "controllerd", "--controller-data-directory", str(tmp_path / "mzdata")],
    )
    with pytest.raises(UIError, match="requires state separate"):
        run.main()
    launcher[2].assert_not_called()


@pytest.mark.parametrize(
    "version",
    [
        "adapterd v1",
        "written-plan-build: ",
        "written-plan-build: 1\nwritten-plan-build: 2",
    ],
)
def test_controller_requires_reported_plan_build(launcher, version):
    launcher[1].return_value = version
    with pytest.raises(UIError, match="written-plan-build"):
        run.main()
    launcher[2].assert_not_called()


def test_controller_external_pubsub_does_not_gate_startup(launcher, monkeypatch):
    monkeypatch.setattr(
        run.sys, "argv", ["run", "controllerd", "--pubsub-url", "http://localhost:7889"]
    )
    launcher[4].side_effect = ConnectionRefusedError("refused")
    assert run.main() == 0
    launcher[4].assert_not_called()
    command = launcher[5].call_args.args[0]
    assert "--persist-pubsub-external" in command
    assert "--persist-pubsub-url=http://localhost:7889" in command
    assert not any(arg.startswith("--pubsubd-image=") for arg in command)


def test_controller_overrides(launcher, monkeypatch):
    monkeypatch.setattr(
        run.sys,
        "argv",
        [
            "run",
            "controllerd",
            "--postgres",
            "postgres://localhost/controller?sslmode=require",
            "--blob",
            "file:///separate/blob",
            "--pubsub-url",
            "http://localhost:7889",
            "--adapter-sizes",
            '{"1":{"cpu_limit":2,"memory_limit":"8 GiB"}}',
            "--",
            "--controller-http-listen-addr=127.0.0.1:7880",
        ],
    )
    assert run.main() == 0
    command = launcher[5].call_args.args[0]
    assert "--persist-blob-url=file:///separate/blob" in command
    assert "--persist-pubsub-url=http://localhost:7889" in command
    assert "--controller-http-listen-addr=127.0.0.1:7880" in command
    assert '--adapter-sizes={"1":{"cpu_limit":2,"memory_limit":"8 GiB"}}' in command
    url = next(
        arg.split("=", 1)[1]
        for arg in command
        if arg.startswith("--persist-consensus-url=")
    )
    assert parse_qs(urlparse(url).query) == {
        "sslmode": ["require"],
        "options": ["--search_path=consensus"],
    }


def test_cargo_build_includes_controller_services(monkeypatch, tmp_path):
    monkeypatch.setattr(run, "MZ_ROOT", tmp_path)
    spawn = MagicMock(return_value=argparse.Namespace(returncode=0))
    monkeypatch.setattr(run.spawn, "runv", spawn)
    args = argparse.Namespace(
        channel=None,
        release=False,
        optimized=True,
        timings=False,
        no_default_features=False,
        sanitizer="none",
        coverage=False,
        features=None,
    )
    status, artifacts = run._cargo_build(
        args, extra_programs=["adapterd", "pubsubd", "controllerd"]
    )
    assert status == 0
    assert spawn.call_args.args[0] == [
        "cargo",
        "build",
        "--profile",
        "optimized",
        "--bin",
        "clusterd",
        "--bin",
        "adapterd",
        "--bin",
        "pubsubd",
        "--bin",
        "controllerd",
    ]
    assert artifacts == [
        str(tmp_path / "target/optimized" / name)
        for name in ("clusterd", "adapterd", "pubsubd", "controllerd")
    ]


def test_controller_signals_only_controller(monkeypatch):
    proc = MagicMock()
    popen = MagicMock(return_value=proc)
    monkeypatch.setattr(run.subprocess, "Popen", popen)
    handlers = {}

    def install(sig, handler):
        handlers[sig] = handler
        return signal.SIG_DFL

    monkeypatch.setattr(run.signal, "signal", install)
    killpg = MagicMock(side_effect=AssertionError("must not kill children"))
    monkeypatch.setattr(run.os, "killpg", killpg)

    def wait():
        handlers[signal.SIGINT](signal.SIGINT, None)
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return 23

    proc.wait.side_effect = wait
    assert run._run_controller(["controllerd"], {}) == 23
    popen.assert_called_once_with(["controllerd"], env={}, start_new_session=True)
    assert [call.args[0] for call in proc.send_signal.call_args_list] == [
        signal.SIGINT,
        signal.SIGTERM,
    ]
    assert set(handlers.values()) == {signal.SIG_DFL}
    killpg.assert_not_called()


@pytest.mark.parametrize("reset", [False, True])
def test_environmentd_retains_setup_and_reset_behavior(monkeypatch, tmp_path, reset):
    monkeypatch.setattr(run, "MZ_ROOT", tmp_path)
    data = tmp_path / "mzdata"
    data.mkdir()
    (data / "environment-id").write_text("existing-environment")
    monkeypatch.setattr(run, "MZDATA", data)
    for key in ("MZDEV_POSTGRES", "MZDEV_BLOB"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(
        run.sys, "argv", ["run", "environmentd"] + (["--reset"] if reset else [])
    )
    monkeypatch.setattr(
        run,
        "_cargo_build",
        MagicMock(
            return_value=(
                0,
                [
                    str(tmp_path / "target/debug/clusterd"),
                    str(tmp_path / "target/debug/environmentd"),
                ],
            )
        ),
    )
    connect = MagicMock()
    monkeypatch.setattr(run, "_connect_sql", connect)
    sql = MagicMock()
    monkeypatch.setattr(run, "_run_sql", sql)
    lingering = MagicMock()
    monkeypatch.setattr(run, "_handle_lingering_services", lingering)
    fork = MagicMock(return_value=0)
    monkeypatch.setattr(run.os, "fork", fork)
    setpgid = MagicMock()
    monkeypatch.setattr(run.os, "setpgid", setpgid)
    launch = MagicMock(return_value=argparse.Namespace(returncode=0))
    monkeypatch.setattr(run.subprocess, "run", launch)
    with pytest.raises(SystemExit) as exited:
        run.main()
    assert exited.value.code == 0
    connect.assert_called_once_with(run.DEFAULT_POSTGRES)
    lingering.assert_called_once_with(kill=reset)
    setpgid.assert_called_once_with(0, 0)
    statements = [call.args[1] for call in sql.call_args_list]
    expected = []
    for schema in ("consensus", "tsoracle", "storage"):
        if reset:
            expected.append(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        expected.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    assert statements == expected
    command = launch.call_args.args[0]
    assert "--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0" in command
    assert f"--orchestrator-process-scratch-directory={tmp_path}/scratch" in command
    assert f"--persist-blob-url={run.DEFAULT_BLOB}" in command
    assert "--persist-pubsub-external" not in command
    assert launch.call_args.kwargs["env"]["RUST_MIN_STACK"] == run.RUST_MIN_STACK
    if reset:
        assert (data / "environment-id").read_text() != "existing-environment"
    else:
        assert "--environment-id=existing-environment" in command


def test_controller_stop_without_initialization(launcher, monkeypatch, tmp_path):
    data = tmp_path / "not-started"
    monkeypatch.setattr(
        run.sys,
        "argv",
        ["run", "controllerd", "--stop", "--controller-data-directory", str(data)],
    )
    assert run.main() == 0
    for mock in launcher:
        mock.assert_not_called()
    assert not data.exists()


@pytest.mark.parametrize("record_location", [False, True])
def test_controller_stop_order_scope_and_repeat(monkeypatch, tmp_path, record_location):
    data = tmp_path / "environment"
    data.mkdir()
    environment_id = f"local-az1-{uuid.uuid4()}-0"
    (data / "environment-id").write_text(environment_id)
    metadata = tmp_path / f"environmentd-{environment_id}"
    service = metadata / "persist-pubsub-relay"
    service.mkdir(parents=True)
    if record_location:
        (data / "process-metadata-directory").write_text(str(metadata))
    (data / "retained-data").write_text("do not delete")
    marker = tmp_path / "controller-stopped"
    ready = tmp_path / "controller-ready"
    processes = []
    try:
        managed = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"]
        )
        external = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"]
        )
        other = subprocess.Popen(
            [
                "controllerd",
                "-c",
                "import time; time.sleep(120)",
                "--environment-id=another-environment",
                f"--orchestrator-process-secrets-directory={data}/secrets",
            ],
            executable=sys.executable,
        )
        processes.extend([managed, external, other])
        start = int(psutil.Process(managed.pid).create_time())
        pid_file = service / "0.pid"
        pid_file.write_text(f"{managed.pid}\n{start}\n")
        script = f"""import os, pathlib, signal, sys, time
def stop(*args):
    os.kill({managed.pid}, 0)
    pathlib.Path({str(marker)!r}).write_text('child still alive')
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
pathlib.Path({str(ready)!r}).write_text('ready')
while True: time.sleep(1)
"""
        controller = subprocess.Popen(
            [
                "controllerd",
                "-c",
                script,
                f"--environment-id={environment_id}",
                f"--orchestrator-process-secrets-directory={data}/secrets",
            ],
            executable=sys.executable,
            env={**os.environ, "TMPDIR": str(tmp_path)},
        )
        processes.append(controller)
        deadline = time.monotonic() + 10
        while not ready.exists():
            assert controller.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.01)
        monkeypatch.setattr(
            run.sys,
            "argv",
            ["run", "controllerd", "--stop", "--controller-data-directory", str(data)],
        )
        monkeypatch.setattr(
            run,
            "_cargo_build",
            MagicMock(side_effect=AssertionError("stop must not build")),
        )
        monkeypatch.setattr(
            run,
            "_connect_sql",
            MagicMock(side_effect=AssertionError("stop must not initialize")),
        )
        assert run.main() == 0
        assert marker.read_text() == "child still alive"
        assert controller.poll() is not None
        assert managed.poll() is not None
        assert external.poll() is None and other.poll() is None
        assert (data / "retained-data").read_text() == "do not delete"
        assert (data / "environment-id").read_text() == environment_id
        assert pid_file.exists()
        assert (data / "process-metadata-directory").exists() == record_location
        assert run.main() == 0
        assert external.poll() is None and other.poll() is None
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)


def test_controller_stop_rejects_stale_and_invalid_identities(tmp_path):
    data = tmp_path / "environment"
    data.mkdir()
    environment_id = f"local-az1-{uuid.uuid4()}-0"
    (data / "environment-id").write_text(environment_id)
    metadata = tmp_path / f"environmentd-{environment_id}"
    service = metadata / "adapter-a1"
    service.mkdir(parents=True)
    (data / "process-metadata-directory").write_text(str(metadata))
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        stale = int(psutil.Process(process.pid).create_time()) - 1
        (service / "0.pid").write_text(f"{process.pid}\n{stale}\n")
        assert run._stop_controller_environment(data) == 0
        assert process.poll() is None
        (service / "0.pid").write_text("0\n0\n")
        with pytest.raises(UIError, match="invalid process identity"):
            run._stop_controller_environment(data)
        assert process.poll() is None
    finally:
        process.terminate()
        process.wait(timeout=10)


def test_controller_metadata_location_survives_shell_changes(tmp_path):
    data = tmp_path / "environment"
    data.mkdir()
    environment_id = f"local-az1-{uuid.uuid4()}-0"
    (data / "environment-id").write_text(environment_id)
    original = {"TMPDIR": str(tmp_path / "original")}
    run._record_controller_metadata_directory(data, original)
    changed = {"TMPDIR": str(tmp_path / "different")}
    run._record_controller_metadata_directory(data, changed)
    assert changed["TMPDIR"] == original["TMPDIR"]
    assert (
        run._controller_metadata_directory(data, environment_id).parent
        == tmp_path / "original"
    )


def test_controller_stop_partial_initialization(monkeypatch, tmp_path):
    data = tmp_path / "environment"
    data.mkdir()
    (data / "environment-id").write_text(f"local-az1-{uuid.uuid4()}-0")
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    before = list(data.iterdir())
    assert run._stop_controller_environment(data) == 0
    assert run._stop_controller_environment(data) == 0
    assert list(data.iterdir()) == before


def test_controller_stop_bad_record_does_not_leave_valid_process(tmp_path):
    data = tmp_path / "environment"
    data.mkdir()
    environment_id = f"local-az1-{uuid.uuid4()}-0"
    (data / "environment-id").write_text(environment_id)
    metadata = tmp_path / f"environmentd-{environment_id}"
    service = metadata / "persist-pubsub-relay"
    service.mkdir(parents=True)
    (data / "process-metadata-directory").write_text(str(metadata))
    (service / "0.pid").write_text("123\n0\n")
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        start = int(psutil.Process(process.pid).create_time())
        (service / "1.pid").write_text(f"{process.pid}\n{start}\n")
        with pytest.raises(UIError, match="Stopped verified processes"):
            run._stop_controller_environment(data)
        assert process.poll() is not None
        assert (service / "0.pid").read_text() == "123\n0\n"
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)
