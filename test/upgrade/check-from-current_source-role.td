# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT role.name AS role, member.name AS member, grantor.name AS grantor FROM mz_role_members membership LEFT JOIN mz_roles role ON membership.role_id = role.id LEFT JOIN mz_roles member ON membership.member = member.id LEFT JOIN mz_roles grantor ON membership.grantor = grantor.id;
group  joe  mz_system

> DROP ROLE superuser_login;

> DROP ROLE "space role";

> DROP ROLE joe;

> DROP ROLE group;
