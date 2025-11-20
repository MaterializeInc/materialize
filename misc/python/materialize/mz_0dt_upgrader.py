from dataclasses import dataclass
from random import Random
from typing import Callable, Iterator, TypedDict

from materialize.mz_version import MzVersion
from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import DeploymentStatus, Materialized

from materialize.mzcompose.services.postgres import METADATA_STORE


class MaterializedUpgradeArgs(TypedDict):
    """Arguments for the Materialized service constructor required for 0dt upgrades."""

    name: str
    image: str | None
    deploy_generation: int
    system_parameter_defaults: dict[str, str]
    external_metadata_store: bool
    restart: str


@dataclass
class UpgradeStep:
    """Represents a single upgrade step with its service name and action."""

    service_name: str
    upgrade: Callable[[], None]


def generate_materialized_upgrade_args(
    versions: list[MzVersion | None],
) -> list[MaterializedUpgradeArgs]:
    """
    Constructs a list of required Materialized arguments for 0dt upgrades.
    Requires there to be an mz_1 and mz_2 service already in the composition.
    """
    # We use the first version to get the system parameters since the defaults for
    # newer versions include cutting edge features than can break backwards compatibility.
    # TODO (multiversion1): Get minimal system parameters by default to avoid cutting edge features.
    system_parameter_defaults = get_default_system_parameters(versions[0])

    return [
        MaterializedUpgradeArgs(
            image=f"materialize/materialized:{version}" if version else None,
            # Cycle through mz_1 and mz_2 for upgrades since spinning up services have a cost.
            name=f"mz_{(i % 2) + 1}",
            # Generation number for the service. Required to start services in read only mode.
            deploy_generation=i,
            system_parameter_defaults=system_parameter_defaults,
            # To share the same metadata store between services
            external_metadata_store=True,
            # To restart when container exits due to promotion
            restart="on-failure",
        )
        for i, version in enumerate(versions)
    ]


def generate_random_upgrade_path(
    versions: list[MzVersion],
    rng: Random | None = None,
) -> list[MzVersion]:
    """
    Generates a random upgrade path between the given versions.
    """
    selected_versions = []

    rng = rng or Random()
    # For each version in the input list, randomly select it with a 50% chance.
    for v in versions:
        if rng.random() < 0.5:
            selected_versions.append(v)

    # Always include at least one version to avoid empty paths.
    if len(selected_versions) == 0:
        selected_versions.append(rng.choice(versions))

    return selected_versions


class Materialized0dtUpgrader:
    """
    Manages a sequence of Materialized service upgrades using zero-downtime deployments.

    Args:
        materialized_services: List of Materialized instances representing each upgrade step
    """

    def __init__(self, c: Composition, materialized_services: list[Materialized]):
        self.materialized_services = materialized_services
        self.c = c

    def _upgrade_steps_iterator(self) -> Iterator[UpgradeStep]:
        """
        Returns an iterator over upgrade step actions from the second service onward.

        Each step is a closure that, when called, will perform
        the upgrade step to the corresponding service.
        """

        def create_upgrade_action(
            current_service: Materialized,
            previous_service: Materialized,
        ):
            def upgrade() -> None:
                with self.c.override(current_service):
                    current_service_image = (
                        current_service.config.get("image") or "current"
                    )
                    previous_service_image = previous_service.config.get("image")

                    print(f"Bringing up {current_service_image}")
                    self.c.up(current_service.name)
                    print(f"Awaiting promotion of {current_service_image}")
                    self.c.await_mz_deployment_status(
                        DeploymentStatus.READY_TO_PROMOTE, current_service.name
                    )
                    self.c.promote_mz(current_service.name)
                    print(f"Awaiting leader status of {current_service_image}")
                    self.c.await_mz_deployment_status(
                        DeploymentStatus.IS_LEADER, current_service.name
                    )

                    print(f"Killing {previous_service_image}")
                    self.c.kill(previous_service.name, wait=True)

            return upgrade

        services = self.materialized_services
        for idx in range(1, len(services)):
            current_service = services[idx]
            previous_service = services[idx - 1]

            yield UpgradeStep(
                service_name=current_service.name,
                upgrade=create_upgrade_action(current_service, previous_service),
            )

    def initialize(self) -> tuple[str, Iterator[UpgradeStep]]:
        """
        Initialize the with the first service. Returns an iterator where
        each step is a closure that, when called, will perform the upgrade step to the corresponding service.
        """
        first_service = self.materialized_services[0]
        with self.c.override(first_service):
            print(f"Bringing up {first_service.name}")
            self.c.up(first_service.name)

        return first_service.name, self._upgrade_steps_iterator()

    def print_upgrade_path(self) -> None:
        """
        Print the upgrade steps.
        """

        def image_to_string(image: str | None) -> str:
            return "current" if image is None else image.split(":")[-1]

        print(
            f"Upgrading through versions {str.join(' -> ', [image_to_string(service.config.get('image')) for service in self.materialized_services])}"
        )

    def cleanup(self) -> None:
        """
        Cleanup after upgrade.
        """
        print("Cleaning up upgrade path")
        # Ensure all services are killed and removed
        self.c.kill(
            *[service.name for service in self.materialized_services], wait=True
        )
        self.c.rm(
            *[service.name for service in self.materialized_services],
            destroy_volumes=True,
        )
        self.c.kill(METADATA_STORE, wait=True)
        self.c.rm(
            METADATA_STORE,
            destroy_volumes=True,
        )
