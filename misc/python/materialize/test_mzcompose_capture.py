# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import gc
import os
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from materialize import mzbuild
from materialize.mzcompose.composition import Composition
from materialize.ui import CommandFailureCausedUIError


@pytest.mark.skipif(not Path("/proc/self/fd").exists(), reason="requires procfs")
@pytest.mark.parametrize("exit_code", [0, 7])
def test_capture_releases_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    exit_code: int,
) -> None:
    monkeypatch.delenv("MZCOMPOSE_CGROUP_PARENT", raising=False)
    (tmp_path / "mzcompose.py").write_text("SERVICES = []\n")
    docker = tmp_path / "docker"
    docker.write_text("""#!/usr/bin/env python3
import sys
sys.stdout.write("out\\ntail")
sys.stderr.write("err\\ntail")
sys.exit(int(sys.argv[-1]))
""")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    repo = cast(
        mzbuild.Repository,
        SimpleNamespace(
            compositions={"capture-test": tmp_path},
            rd=SimpleNamespace(coverage=False),
        ),
    )
    c = Composition(repo, "capture-test", resolve_image_specs=False, silent=True)
    c.invoke("0", capture_and_print=True)
    capsys.readouterr()
    gc.collect()
    before = len(list(Path("/proc/self/fd").iterdir()))
    enabled = gc.isenabled()
    # Closing OS resources must not depend on cyclic garbage collection.
    gc.disable()
    try:
        for _ in range(20):
            if exit_code:
                with pytest.raises(CommandFailureCausedUIError) as error:
                    c.invoke(str(exit_code), capture_and_print=True)
                assert error.value.stdout == "out\ntail"
                assert error.value.stderr == "err\ntail"
            else:
                result = c.invoke(str(exit_code), capture_and_print=True)
                assert result.stdout == "out\ntail"
                assert result.stderr == "err\ntail"
            printed = capsys.readouterr()
            assert printed.out == "out\ntail"
            assert printed.err == "err\ntail"
        assert len(list(Path("/proc/self/fd").iterdir())) == before
    finally:
        if enabled:
            gc.enable()
        gc.collect()
