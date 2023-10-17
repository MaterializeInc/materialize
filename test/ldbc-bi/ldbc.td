# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the cardinality of LDBC tables.

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC
contains: multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR 2) FOR ALL TABLES
contains: unsupported scale factor: 2

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR -1) FOR ALL TABLES
contains: unsupported scale factor: -1

> CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR 1) FOR ALL TABLES

> SELECT count(*) FROM comment
1739438

> SELECT count(*) FROM comment_hastag_tag
2176131

> SELECT count(*) FROM forum
100827

> SELECT count(*) FROM forum_hasmember_person
2909768

> SELECT count(*) FROM forum_hastag_tag
328584

> SELECT count(*) FROM organisation
7954

> SELECT count(*) FROM person
10295

> SELECT count(*) FROM person_hasinterest_tag
238052

> SELECT count(*) FROM person_knows_person
346028

> SELECT count(*) FROM person_likes_comment
1109813

> SELECT count(*) FROM person_likes_post
760455

> SELECT count(*) FROM person_studyat_university
8309

> SELECT count(*) FROM person_workat_company
22044

> SELECT count(*) FROM place
1460

> SELECT count(*) FROM post
1121226

> SELECT count(*) FROM post_hastag_tag
751933

> SELECT count(*) FROM tag
16080

> SELECT count(*) FROM tagclass
71
