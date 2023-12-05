# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The non-stuck sub-source is fine...
> SELECT COUNT(*) FROM two;
11

# The above select statement should ensure that the persist-sink for `one` also
# received the "new" snapshot, ensuring we don't double count, but we add additional
# sleeping here to ensure that we aren't just reading the original snapshot committed
# to `one`.
> SELECT mz_unsafe.mz_sleep(10)
<null>

# But the other one has duplicated data!
> SELECT COUNT(*) FROM one;
11
