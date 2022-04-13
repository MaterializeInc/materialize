# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE ROLE superuser_login SUPERUSER LOGIN;

> CREATE ROLE "space role" LOGIN SUPERUSER;

# If this begins to succeeed, this means additional tests need to be added
! CREATE ROLE nosuperuser_login NOSUPERUSER LOGIN;
contains:non-superusers not yet supported

! CREATE ROLE superuser_nologin SUPERUSER NOLOGIN;
contains:non-login users not yet supported
