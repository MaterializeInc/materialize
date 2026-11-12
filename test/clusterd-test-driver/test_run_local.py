# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Process-lifetime cleanup checks without building or starting databases."""

import importlib.util
import os
import select
import signal
import subprocess
import sys
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "native_driver_run_local", Path(__file__).with_name("run-local.py")
)
assert spec is not None and spec.loader is not None
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


@unittest.skipUnless(os.name == "posix", "requires Unix process groups")
class CleanupTest(unittest.TestCase):
    def test_reaps_descendant_even_after_wrapper_exits(self) -> None:
        descendant = (
            "import signal,time; "
            "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
            "print('ready',flush=True); time.sleep(60)"
        )
        for wrapper_exits in (False, True):
            with self.subTest(wrapper_exits=wrapper_exits):
                wrapper = (
                    "import subprocess,sys; "
                    f"p=subprocess.Popen([sys.executable,'-c',{descendant!r}]); "
                    + ("sys.exit(0)" if wrapper_exits else "p.wait()")
                )
                with subprocess.Popen(
                    [sys.executable, "-c", wrapper],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                ) as child:
                    try:
                        assert child.stdout is not None
                        self.assertTrue(select.select([child.stdout], [], [], 5)[0])
                        self.assertEqual(child.stdout.readline(), b"ready\n")
                        if wrapper_exits:
                            self.assertEqual(child.wait(timeout=5), 0)
                        runner.terminate_group(child, grace=0.1)
                        # The descendant inherits stdout. EOF proves cleanup did
                        # not merely reap the wrapper while leaving it alive.
                        child.communicate(timeout=5)
                    finally:
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        child.wait()


if __name__ == "__main__":
    unittest.main()
