# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE VIEW data AS SELECT * FROM (VALUES ('asdfjkl'), ('foo'), ('asdf'), (NULL), ('jkl'))

> SELECT data.*, reg.* FROM data, regex('(asdf)|(?P<foo>jkl)', data.column1) reg ORDER BY data.column1
column1 column1 foo
-------------------
asdf asdf <null>
asdfjkl asdf <null>
jkl <null> jkl

# TODO - Test that the columns have the correct nullability, once they actually do (#1685)
