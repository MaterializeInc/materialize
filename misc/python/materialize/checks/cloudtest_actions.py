# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import Any

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.environmentd import EnvironmentdStatefulSet
from materialize.mz_version import MzVersion


class ReplaceEnvironmentdStatefulSet(Action):
    """Change the image tag of the environmentd stateful set, re-create the definition and replace the existing one."""

    new_tag: str | None

    def __init__(self, new_tag: str | None = None) -> None:
        self.new_tag = new_tag

    def execute(self, e: Executor) -> None:
        new_version = (
            MzVersion.parse_mz(self.new_tag)
            if self.new_tag
            else MzVersion.parse_cargo()
        )
        print(
            f"Replacing environmentd stateful set from version {e.current_mz_version} to version {new_version}"
        )
        mz = e.cloudtest_application()
        stateful_set = [
            resource
            for resource in mz.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.tag = self.new_tag
        stateful_set.replace()
        e.current_mz_version = new_version

    def join(self, e: Executor) -> None:
        # execute is blocking already
        pass


class SetupSshTunnels(Action):
    """Prepare the SSH tunnels."""

    def __init__(self, mz: MaterializeApplication) -> None:
        self.handle: Any | None = None
        self.mz = mz

    def execute(self, e: Executor) -> None:
        connection_count = 4
        self.handle = e.testdrive(
            "\n".join(
                [
                    dedent(
                        f"""
                        > CREATE CONNECTION IF NOT EXISTS ssh_tunnel_{i} TO SSH TUNNEL (
                            HOST 'ssh-bastion-host',
                            USER 'mz',
                            PORT 23
                            );
                        """
                    )
                    for i in range(connection_count)
                ]
            )
        )

        for i in range(connection_count):
            public_key = self.mz.environmentd.sql_query(
                "SELECT public_key_1 FROM mz_ssh_tunnel_connections ssh"
                " JOIN mz_connections c ON c.id = ssh.id"
                f" WHERE c.name = 'ssh_tunnel_{i}';"
            )[0][0]

            # Add public key to SSH bastion host
            self.mz.kubectl(
                "exec",
                "svc/ssh-bastion-host",
                "--",
                "bash",
                "-c",
                f"echo '{public_key}' >> /etc/authorized_keys/mz",
            )

    def join(self, e: Executor) -> None:
        e.join(self.handle)
