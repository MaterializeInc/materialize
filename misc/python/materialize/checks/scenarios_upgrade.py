# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from dataclasses import dataclass
from materialize.checks.actions import Action, Initialize, Manipulate, Sleep, Validate
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.checks.features import Features
from materialize.checks.mzcompose_actions import (
    KillClusterdCompute,
    KillMz,
    PromoteMz,
    StartClusterdCompute,
    StartMz,
    UseClusterdCompute,
    WaitReadyMz,
)
from materialize.checks.scenarios import Scenario
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.materialized import LEADER_STATUS_HEALTHCHECK
from materialize.version_list import (
    fetch_self_managed_versions,
    get_published_minor_mz_versions,
    get_self_managed_versions,
)

# late initialization
_minor_versions: list[MzVersion] | None = None
_last_version: MzVersion | None = None
_previous_version: MzVersion | None = None


def get_minor_versions() -> list[MzVersion]:
    global _minor_versions
    if _minor_versions is None:
        current_version = MzVersion.parse_cargo()
        _minor_versions = [
            v
            for v in get_published_minor_mz_versions(exclude_current_minor_version=True)
            if v < current_version
        ]
    return _minor_versions


def get_last_version() -> MzVersion:
    global _last_version
    if _last_version is None:
        _last_version = get_minor_versions()[0]
    return _last_version


def get_previous_version() -> MzVersion:
    global _previous_version
    if _previous_version is None:
        _previous_version = get_minor_versions()[1]
    return _previous_version


def start_mz_read_only(
    scenario: Scenario,
    deploy_generation: int,
    mz_service: str = "materialized",
    tag: MzVersion | None = None,
    system_parameter_defaults: dict[str, str] | None = None,
    system_parameter_version: MzVersion | None = None,
    force_migrations: str | None = None,
    publish: bool | None = None,
) -> StartMz:
    return StartMz(
        scenario,
        tag=tag,
        mz_service=mz_service,
        deploy_generation=deploy_generation,
        healthcheck=LEADER_STATUS_HEALTHCHECK,
        restart="on-failure",
        system_parameter_defaults=system_parameter_defaults,
        system_parameter_version=system_parameter_version,
        force_migrations=force_migrations,
        publish=publish,
    )


class UpgradeEntireMzFromLatestSelfManaged(Scenario):
    """Upgrade the entire Mz instance from the last Self-Managed version without any intermediate steps. This makes sure our Self-Managed releases for self-managed Materialize stay upgradable."""

    def base_version(self) -> MzVersion:
        return get_self_managed_versions()[-1]

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            StartMz(
                self,
                tag=None,
            ),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class UpgradeEntireMzFromPreviousSelfManaged(Scenario):
    """Upgrade the entire Mz instance through the last two Self-Managed versions. This makes sure our Self-Managed releases for self-managed Materialize stay upgradable."""

    def base_version(self) -> MzVersion:
        return get_self_managed_versions()[-2]

    def actions(self) -> list[Action]:
        print(
            f"Upgrading from tag {self.base_version()} through {get_self_managed_versions()[-1]}"
        )
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            StartMz(self, tag=get_self_managed_versions()[-1]),
            Manipulate(self, phase=2),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
            # A second restart while already on the new version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class UpgradeEntireMz(Scenario):
    """Upgrade the entire Mz instance from the last released version."""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            StartMz(
                self,
                tag=None,
            ),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class UpgradeEntireMzTwoVersions(Scenario):
    """Upgrade the entire Mz instance starting from the previous
    released version and passing through the last released version."""

    def base_version(self) -> MzVersion:
        return get_previous_version()

    def actions(self) -> list[Action]:
        print(f"Upgrade path: {self.base_version()} -> {get_last_version()} -> current")
        return [
            # Start with previous_version
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            # Upgrade to last_version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=get_last_version(),
            ),
            Manipulate(self, phase=1),
            # Upgrade to current source
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
            ),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the current source
            KillMz(),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class UpgradeEntireMzFourVersions(Scenario):
    """Test upgrade X-4 -> X-3 -> X-2 -> X-1 -> X"""

    def __init__(
        self,
        checks: list[type[Check]],
        executor: Executor,
        features: Features,
        seed: str | None = None,
    ):
        self.minor_versions = get_minor_versions()
        super().__init__(checks, executor, features, seed)

    def base_version(self) -> MzVersion:
        return self.minor_versions[3]

    def actions(self) -> list[Action]:
        print(
            f"Upgrade path: {self.minor_versions[3]} -> {self.minor_versions[2]} -> {get_previous_version()} -> {get_last_version()} -> current"
        )
        return [
            StartMz(
                self,
                tag=self.minor_versions[3],
            ),
            Initialize(self),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=self.minor_versions[2],
            ),
            Manipulate(self, phase=1),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=get_previous_version(),
            ),
            Manipulate(self, phase=2),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=get_last_version(),
            ),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
            KillMz(),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


#
# We are limited with respect to the different orders in which stuff can be upgraded:
# - some sequences of events are invalid
# - environmentd and storage clusterds are located in the same container
#
# Still, we would like to try as many scenarios as we can
#


class UpgradeClusterdComputeLast(Scenario):
    """Upgrade compute's clusterd separately after upgrading environmentd"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            StartClusterdCompute(tag=self.base_version()),
            UseClusterdCompute(self),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
                system_parameter_version=self.base_version(),
            ),
            # No useful work can be done while clusterd is old-version
            # and environmentd is new-version. So we proceed
            # to upgrade clusterd as well.
            # We sleep here to allow some period of coexistence, even
            # though we are not issuing queries during that time.
            Sleep(10),
            KillClusterdCompute(capture_logs=True),
            StartClusterdCompute(tag=None),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class UpgradeClusterdComputeFirst(Scenario):
    """Upgrade compute's clusterd separately before environmentd"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            StartClusterdCompute(tag=self.base_version()),
            UseClusterdCompute(self),
            Initialize(self),
            Manipulate(self, phase=1),
            KillClusterdCompute(capture_logs=True),
            StartClusterdCompute(tag=None),
            # No useful work can be done while clusterd is new-version
            # and environmentd is old-version. So we
            # proceed to upgrade them as well.
            # We sleep here to allow some period of coexistence, even
            # though we are not issuing queries during that time.
            Sleep(10),
            KillMz(),
            StartMz(
                self,
                tag=None,
            ),
            Manipulate(self, phase=2),
            Validate(self),
            KillMz(),
            StartMz(
                self,
                tag=None,
            ),
            Validate(self),
        ]


class PreflightCheckContinue(Scenario):
    """Preflight check, then upgrade"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def _include_check_class(self, check_class: type[Check]) -> bool:
        if not super()._include_check_class(check_class):
            return False

        return True

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            start_mz_read_only(self, tag=None, deploy_generation=1),
            WaitReadyMz(),
            PromoteMz(),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(),
            StartMz(
                self,
                tag=None,
                deploy_generation=1,
            ),
            Validate(self),
        ]


class PreflightCheckRollback(Scenario):
    """Preflight check, then roll back"""

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            start_mz_read_only(
                self,
                tag=None,
                deploy_generation=1,
                system_parameter_version=self.base_version(),
            ),
            WaitReadyMz(),
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while still on old version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Validate(self),
        ]


class ActivateSourceVersioningMigration(Scenario):
    """
    Starts MZ, initializes and manipulates, then forces the migration
    of sources to the new table model (introducing Source Versioning).
    """

    def base_version(self) -> MzVersion:
        return get_last_version()

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")
        return [
            StartMz(
                self,
                tag=self.base_version(),
            ),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(
                capture_logs=True
            ),  #  We always use True here otherwise docker-compose will lose the pre-upgrade logs
            StartMz(
                self,
                tag=None,
                # Activate the `force_source_table_syntax` flag
                # which should trigger the migration of sources
                # using the old syntax to the new table model.
                additional_system_parameter_defaults={
                    "force_source_table_syntax": "true",
                },
            ),
            Manipulate(self, phase=2),
            Validate(self),
            # A second restart while already on the new version
            KillMz(capture_logs=True),
            StartMz(
                self,
                tag=None,
                additional_system_parameter_defaults={
                    "force_source_table_syntax": "true",
                },
            ),
            Validate(self),
        ]


class SelfManagedv25_2_Upgrade(Scenario):
    """
    Upgrade from the oldest v25.2 patch release to the latest v25.2 patch release to main.
    """

    def __init__(
        self,
        checks: list[type[Check]],
        executor: Executor,
        features: Features,
        seed: str | None = None,
    ):
        self_managed_versions = fetch_self_managed_versions()
        self.v25_2_versions = sorted(
            [
                v.version
                for v in self_managed_versions
                if v.helm_version.major == 25 and v.helm_version.minor == 2
            ]
        )

        super().__init__(checks, executor, features, seed)

    def base_version(self) -> MzVersion:
        return self.v25_2_versions[0]

    def actions(self) -> list[Action]:
        print(f"Upgrading from tag {self.base_version()}")

        def upgrade_actions(
            version: MzVersion | None,
            deploy_generation: int,
            service_name: str,
            previous_service_name: str,
        ) -> list[Action]:
            return [
                start_mz_read_only(
                    self,
                    tag=version,
                    deploy_generation=deploy_generation,
                    mz_service=service_name,
                ),
                WaitReadyMz(service_name),
                PromoteMz(service_name),
                Validate(self, mz_service=service_name),
                KillMz(capture_logs=True, mz_service=previous_service_name),
            ]

        @dataclass
        class MzServiceInfo:
            version: MzVersion | None
            service_name: str

        actions = []
        versions = self.v25_2_versions + [None]

        mz_services = [
            MzServiceInfo(
                version=version,
                # We alternate between mz_1 and mz_2
                service_name=f"mz_{(i % 2) + 1}",
            )
            for i, version in enumerate(versions)
        ]

        for i, service in enumerate(mz_services):
            if i == 0:
                actions.extend(
                    [
                        StartMz(
                            self,
                            tag=service.version,
                            mz_service=service.service_name,
                        ),
                        Initialize(self, mz_service=service.service_name),
                        Manipulate(self, phase=1, mz_service=service.service_name),
                        Manipulate(self, phase=2, mz_service=service.service_name),
                        Validate(self, mz_service=service.service_name),
                    ]
                )
            else:
                # Because upgrades start on generation 1, we can set it to the index
                deploy_generation = i
                actions.extend(
                    upgrade_actions(
                        service.version,
                        deploy_generation,
                        service.service_name,
                        mz_services[i - 1].service_name,
                    )
                )

        return actions
