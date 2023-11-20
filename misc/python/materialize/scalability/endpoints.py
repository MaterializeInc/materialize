# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pg8000

from materialize import git
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.scalability.endpoint import Endpoint

POSTGRES_ENDPOINT_NAME = "postgres"


class MaterializeRemote(Endpoint):
    """Connect to a remote Materialize instance using a psql URL"""

    def __init__(self, materialize_url: str) -> None:
        self.materialize_url = materialize_url

    def url(self) -> str:
        return self.materialize_url

    def up(self) -> None:
        pass

    def __str__(self) -> str:
        return f"MaterializeRemote ({self.materialize_url})"


class PostgresContainer(Endpoint):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition
        self._port: int | None = None
        super().__init__()

    def host(self) -> str:
        return "localhost"

    def port(self) -> int:
        assert self._port is not None
        return self._port

    def user(self) -> str:
        return "postgres"

    def password(self) -> str:
        return "postgres"

    def up(self) -> None:
        self.composition.down(destroy_volumes=True)
        with self.composition.override(Postgres()):
            self.composition.up("postgres")
            self._port = self.composition.default_port("postgres")

    def try_load_version(self) -> str:
        return POSTGRES_ENDPOINT_NAME

    def __str__(self) -> str:
        return "PostgresContainer"


class MaterializeNonRemote(Endpoint):
    def host(self) -> str:
        return "localhost"

    def user(self) -> str:
        return "materialize"

    def password(self) -> str:
        return "materialize"

    def priv_port(self) -> int:
        raise NotImplementedError

    def lift_limits(self) -> None:
        priv_conn = pg8000.connect(
            host=self.host(), user="mz_system", port=self.priv_port()
        )
        priv_conn.autocommit = True
        priv_cursor = priv_conn.cursor()
        priv_cursor.execute("ALTER SYSTEM SET max_connections = 65535;")
        priv_cursor.execute("ALTER SYSTEM SET max_tables = 65535;")


class MaterializeLocal(MaterializeNonRemote):
    """Connect to a Materialize instance running on the local host"""

    def port(self) -> int:
        return 6875

    def priv_port(self) -> int:
        return 6877

    def up(self) -> None:
        self.lift_limits()

    def __str__(self) -> str:
        return f"MaterializeLocal ({self.host()})"


class MaterializeContainer(MaterializeNonRemote):
    def __init__(
        self,
        composition: Composition,
        specified_target: str,
        image: str | None = None,
        alternative_image: str | None = None,
    ) -> None:
        self.composition = composition
        self.image = image
        self.alternative_image = (
            alternative_image if image != alternative_image else None
        )
        self._port: int | None = None
        self.specified_target = specified_target
        super().__init__()

    def port(self) -> int:
        assert self._port is not None
        return self._port

    def priv_port(self) -> int:
        return self.composition.port("materialized", 6877)

    def up(self) -> None:
        self.composition.down(destroy_volumes=True)

        print(f"Image is {self.image} (alternative: {self.alternative_image})")

        if self.image is not None and self.alternative_image is not None:
            if not self.composition.try_pull_service_image(
                Materialized(image=self.image)
            ):
                # explicitly specified image cannot be found and alternative exists
                print(
                    f"Unable to find image {self.image}, proceeding with alternative image {self.alternative_image}!"
                )
                self.image = self.alternative_image
            else:
                print(f"Found image {self.image}, proceeding with this image.")

        self.up_internal()
        self.lift_limits()

    def up_internal(self) -> None:
        with self.composition.override(
            Materialized(image=self.image, sanity_restart=False)
        ):
            self.composition.up("materialized")
            self._port = self.composition.default_port("materialized")

    def __str__(self) -> str:
        return (
            f"MaterializeContainer ({self.image} specified as {self.specified_target})"
        )


def endpoint_name_to_description(endpoint_name: str) -> str:
    if endpoint_name == POSTGRES_ENDPOINT_NAME:
        return endpoint_name

    commit_sha = endpoint_name.split(" ")[1].strip("()")

    # empty when mz_version() reports a Git SHA that is not available in the current repository
    commit_message = git.get_commit_message(commit_sha) or "unknown"
    return f"{endpoint_name} - {commit_message}"
