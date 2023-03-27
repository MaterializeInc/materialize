# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import random
import subprocess
from textwrap import dedent
from typing import List, Optional

SYSTEM_ROLES = ("materialize", "mz_system", "mz_introspection")
SYSTEM_DATABASES = ("materialize",)


class DBObject:
    pass


class Database(DBObject):
    def __init__(self, name: str, owner: str):
        self.name = name
        self.owner = owner


class Grant(DBObject):
    def __init__(self, role: "Role", grantor: "Role"):
        self.role = role
        self.grantor = grantor


class Role(DBObject):
    def __init__(
        self,
        name: str,
        createdb: bool,
        createrole: bool,
        createcluster: bool,
        superuser: bool,
    ):
        self.name = name
        self.createdb = createdb
        self.createrole = createrole
        self.createcluster = createcluster
        self.superuser = superuser
        self.grants: List[Grant] = []


class Universe:
    def __init__(self):
        self.roles: List[Role] = [
            Role("materialize", False, False, False, False),
            Role("mz_introspection", False, False, False, False),
            Role("mz_system", True, True, True, True),
        ]
        self.dbs: List[Database] = [Database("materialize", "mz_system")]
        self.rbac_checks = False


class Action:
    def _connect_str(self, role_name: str) -> str:
        if role_name == "materialize":
            return "> "
        if role_name in SYSTEM_ROLES:
            return f"$ postgres-execute connection=postgres://{role_name}:materialize@localhost:6877\n"
        return f"$ postgres-execute connection=postgres://{role_name}@localhost:6875\n"

    def _can_create_db(self, role_name: str, db: Universe) -> bool:
        if role_name == "mz_introspection":
            return False
        if not db.rbac_checks:
            return True
        for role in db.roles:
            if role.name == role_name:
                return role.superuser or role.createdb
        raise ValueError(f"Role {role} not found")

    def _can_create_role(self, role_name: str, db: Universe) -> bool:
        if role_name == "mz_introspection":
            return False
        if not db.rbac_checks:
            return True
        for role in db.roles:
            if role.name == role_name:
                return role.superuser or role.createrole
        raise ValueError(f"Role {role} not found")

    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        raise ValueError("Not implemented")


class CreateRole(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        i = len(db.roles)
        while True:
            if all(f"role{i}" != existing.name for existing in db.roles):
                break
            i += 1
        role = Role(
            f"role{i}",
            rng.choice([True, False]),
            rng.choice([True, False]),
            rng.choice([True, False]),
            False,
        )
        optionals = ["INHERIT"]
        attrs = []
        if role.createdb:
            attrs.append("CREATEDB")
        else:
            optionals.append("NOCREATEDB")
        if role.createrole:
            attrs.append("CREATEROLE")
        else:
            optionals.append("NOCREATEROLE")
        if role.createcluster:
            attrs.append("CREATECLUSTER")
        else:
            optionals.append("NOCREATECLUSTER")
        all_attrs = attrs + rng.sample(optionals, rng.randrange(len(optionals)))
        rng.shuffle(all_attrs)
        cmd = f"CREATE ROLE {role.name} {' '.join(all_attrs)}"
        user = rng.choice(db.roles)
        if not self._can_create_role(user.name, db):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: permission denied to create role"
            return None
        db.roles.append(role)
        return self._connect_str(user.name) + cmd


class DropRole(Action):
    def _owns_objects(self, user: str, db: Universe) -> bool:
        for db2 in db.dbs:
            if db2.owner == user:
                return True
        return False

    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        if not db.roles:
            return None

        role = rng.choice(db.roles)

        if role.name in SYSTEM_ROLES:
            return None

        if_exists = "IF EXISTS " if rng.choice([True, False]) else ""
        cmd = f"DROP ROLE {if_exists}{role.name}"
        user = rng.choice(db.roles)

        if user == role:
            return None

        if not self._can_create_role(user.name, db):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: permission denied to drop roles"
            return None
        if self._owns_objects(role.name, db):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: cannot be dropped because some objects depend on it"
            return None
        db.roles.remove(role)
        for grant in db.roles:
            if grant.name == role.name:
                db.roles.remove(grant)
                continue
            grant.grants = [grant2 for grant2 in grant.grants if grant2.role.name != role.name]
            for grant2 in grant.grants:
                if grant2.grantor.name == role.name:
                    grant2.grantor.name = "<null>"
        return self._connect_str(user.name) + cmd


class ShowRole(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        result = "> SELECT name, inherit, create_role, create_db, create_cluster FROM mz_roles"
        for role in db.roles:
            result += f"\n{role.name} true {str(role.createrole).lower()} {str(role.createdb).lower()} {str(role.createcluster).lower()}"
        return result


class AlterRole(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        if not db.roles:
            return None

        role = rng.choice(db.roles)

        if role.name in SYSTEM_ROLES:
            return None

        user = rng.choice(db.roles)
        can = self._can_create_role(user.name, db)

        cmd = f"ALTER ROLE {role.name}"
        if rng.choice([True, False]):
            if can:
                role.createdb = rng.choice([True, False])
            cmd += " CREATEDB" if role.createdb else " NOCREATEDB"
        if rng.choice([True, False]):
            if can:
                role.createrole = rng.choice([True, False])
            cmd += " CREATEROLE" if role.createrole else " NOCREATEROLE"
        if rng.choice([True, False]):
            if can:
                role.createcluster = rng.choice([True, False])
            cmd += " CREATECLUSTER" if role.createcluster else " NOCREATECLUSTER"
        if not can:
            if user.name == "materialize":
                return f"! {cmd}\ncontains: permission denied to alter role"
            return None
        return self._connect_str(user.name) + cmd


class GrantRole(Action):
    def _has_grant(self, role: Role, grant: Role) -> bool:
        if role == grant:
            return True
        return any([self._has_grant(grant2.role, grant) for grant2 in role.grants])

    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        if not db.roles:
            return None

        grant = rng.choice(db.roles)
        role = rng.choice(db.roles)

        if role.name in SYSTEM_ROLES:
            return None
        if grant.name in SYSTEM_ROLES:
            return None

        cmd = f"GRANT {grant.name} TO {role.name}"
        user = rng.choice(db.roles)
        if not self._can_create_role(user.name, db):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: permission denied to grant role"
            return None
        if self._has_grant(grant, role):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: is a member of role"
            return None
        if not any([grant.name == grant2.role.name for grant2 in role.grants]):
            role.grants.append(Grant(grant, user))
        return self._connect_str(user.name) + cmd


class RevokeRole(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        if not db.roles:
            return None

        for i in range(10):
            role = rng.choice(db.roles)
            if not role.grants:
                continue
            grant = rng.choice(role.grants)
            cmd = f"REVOKE {grant.role.name} FROM {role.name}"
            user = rng.choice(db.roles)
            if not self._can_create_role(user.name, db):
                if user.name == "materialize":
                    return f"! {cmd}\ncontains: permission denied to revoke role"
                return None
            role.grants.remove(grant)
            return self._connect_str(user.name) + cmd


class ShowRoleMembers(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        result = "> SELECT role.name, member_role.name, grantor_role.name FROM mz_role_members LEFT JOIN mz_roles AS role ON role_id = role.id LEFT JOIN mz_roles AS member_role ON member = member_role.id LEFT JOIN mz_roles AS grantor_role ON grantor = grantor_role.id"
        for role in db.roles:
            for grant in role.grants:
                result += f"\n{grant.role.name} {role.name} {grant.grantor.name}"
        return result


class EnableRBACChecks(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        db.rbac_checks = rng.choice([True, False])
        return f"""$ postgres-execute connection=postgres://mz_system:materialize@localhost:6877
ALTER SYSTEM SET enable_rbac_checks TO {str(db.rbac_checks).lower()}"""


class CreateDatabase(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        i = len(db.dbs)
        while True:
            if all(f"db{i}" != existing.name for existing in db.dbs):
                break
            i += 1
        user = rng.choice(db.roles)
        newdb = Database(f"db{i}", user.name)
        if_not_exists = "IF NOT EXISTS " if rng.choice([True, False]) else ""
        cmd = f"CREATE DATABASE {if_not_exists}{newdb.name}"
        if not self._can_create_db(user.name, db):
            if user.name == "materialize":
                return f"! {cmd}\ncontains: permission denied to create database"
            return None
        db.dbs.append(newdb)
        return self._connect_str(user.name) + cmd


class DropDatabase(Action):
    def run(self, db: Universe, rng: random.Random) -> Optional[str]:
        if not db.dbs:
            return None

        db2 = rng.choice(db.dbs)

        if db2.name in SYSTEM_DATABASES:
            return None

        if_exists = "IF EXISTS " if rng.choice([True, False]) else ""
        db.dbs.remove(db2)
        return f"> DROP DATABASE {if_exists}{db2.name}"


ACTIONS = [
    (CreateRole(), 2),
    (DropRole(), 1),
    (ShowRole(), 1),
    (AlterRole(), 1),
    (GrantRole(), 4),
    (RevokeRole(), 2),
    (ShowRoleMembers(), 1),
    (EnableRBACChecks(), 1),
    (CreateDatabase(), 2),
    (DropDatabase(), 2),
]


def reset(db: Universe, rng: random.Random) -> None:
    args = ["target/release/testdrive", "-"]
    result = subprocess.run(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, input="", encoding="utf-8"
    )
    if result.returncode != 0:
        raise ValueError(result.stderr)


def run_action(action: str, db: Universe, rng: random.Random) -> None:
    if action:
        args = [
            "target/release/testdrive",
            "--no-reset",
            "--var=default-replica-size=1",
            "--var=default-storage-size=1",
            "--var=replicas=1",
            "--max-errors=0",
            "--default-max-tries=1",
            "-",
        ]
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            input=action,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise ValueError(result.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", default=None, type=int)
    args = parser.parse_args()

    seed = args.seed or random.randint(0, 2**31)
    rng = random.Random(seed)

    actions: List[str] = []
    db = Universe()
    reset(db, rng)

    try:
        while True:
            action = rng.choices(
                [action[0] for action in ACTIONS], [action[1] for action in ACTIONS]
            )[0]
            txt = action.run(db, rng)
            if txt:
                print(txt)
                actions.append(txt)
                run_action(txt, db, rng)
    except ValueError as e:
        print(f"Found issue, trying to reduce:\n{e}")

        for pos in range(len(actions) - 2, -1, -1):
            new_actions = actions.copy()
            del new_actions[pos]
            reset(db, rng)
            try:
                for action in new_actions:
                    run_action(action, db, rng)
            except ValueError as e2:
                if str(e) == str(e2):
                    print(f"Removing action {pos}/{len(actions)} worked")
                    actions = new_actions
                    continue
            print(f"Removing action {pos}/{len(actions)} didn't work")
        print("Minimal approach found")
        reset(db, rng)
        try:
            for action in actions:
                print(action)
                run_action(action, db, rng)
        except ValueError as e:
            print(str(e))


if __name__ == "__main__":
    main()
