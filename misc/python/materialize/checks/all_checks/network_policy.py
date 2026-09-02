# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class NetworkPolicies(Check):
    """CREATE / ALTER / DROP NETWORK POLICY and SHOW NETWORK POLICIES.

    NOTE: We never point `ALTER SYSTEM SET network_policy` at one of our
    policies. Activating a policy with a mistake in it would lock every
    subsequent connection out of the shared test instance.
    """

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.128.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE NETWORK POLICY network_policy_1 (
                RULES (
                  np1_rule1 (action='allow', direction='ingress', address='1.2.3.4/28')
                )
              )
            > CREATE NETWORK POLICY network_policy_drop (
                RULES (
                  npd_rule1 (action='allow', direction='ingress', address='0.0.0.0/0')
                )
              )
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER NETWORK POLICY network_policy_1 SET (
                    RULES (
                      np1_rule1 (action='allow', direction='ingress', address='1.2.3.4/28'),
                      np1_rule2 (action='allow', direction='ingress', address='5.6.7.8/32')
                    )
                  )
                > DROP NETWORK POLICY network_policy_drop
                > CREATE NETWORK POLICY network_policy_2 (
                    RULES (
                      np2_rule1 (action='allow', direction='ingress', address='10.0.0.0/8')
                    )
                  )
                """,
                """
                > ALTER NETWORK POLICY network_policy_2 SET (
                    RULES (
                      np2_rule1 (action='allow', direction='ingress', address='10.0.0.0/16')
                    )
                  )
                > CREATE NETWORK POLICY network_policy_3 (
                    RULES (
                      np3_rule1 (action='allow', direction='ingress', address='192.168.0.0/24')
                    )
                  )
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT name FROM mz_internal.mz_network_policies WHERE name LIKE 'network_policy_%'
            network_policy_1
            network_policy_2
            network_policy_3

            > SELECT p.name, r.name, r.action, r.direction, r.address
              FROM mz_internal.mz_network_policy_rules r
              JOIN mz_internal.mz_network_policies p ON r.policy_id = p.id
              WHERE p.name LIKE 'network_policy_%'
            network_policy_1 np1_rule1 allow ingress 1.2.3.4/28
            network_policy_1 np1_rule2 allow ingress 5.6.7.8/32
            network_policy_2 np2_rule1 allow ingress 10.0.0.0/16
            network_policy_3 np3_rule1 allow ingress 192.168.0.0/24

            > SELECT count(*) FROM mz_internal.mz_network_policies WHERE name = 'network_policy_drop'
            0
            """))
