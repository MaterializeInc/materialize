# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# version constraint property parsing in testdrive

>[version>=5500] SELECT 1;
1

>[version>=5499] SELECT 2;
2

>[version<=9999999] SELECT 3;
3

>[version<5500] SELECT mz_internal.mz_panic('');
3

>[version<5500] SELECT mz_internal.mz_panic('');
1
