--  Copyright Materialize, Inc. and contributors. All rights reserved.
--
--  Use of this software is governed by the Business Source License
--  included in the LICENSE file at the root of this repository.
--
--  As of the Change Date specified in that file, in accordance with
--  the Business Source License, use of this software will be governed
--  by the Apache License, Version 2.0.
--
--  run — a run of LDBC BI's delta-query sensitive queries

\timing on
\! echo DISABLING PAGER
\pset pager 0
\! date -Iseconds
\! echo INITIALIZING
\i init.sql
\! date -Iseconds
\! echo RUNNING QUERIES
\! echo
\! echo Q10 EXPLAINED
\i e10.sql
\! date -Iseconds
\! echo Q10 RUN 1
\i q10.sql
\! echo Q10 RUN 2
\i q10.sql
\! echo Q10 RUN 3
\i q10.sql
\! echo
\! echo Q11 EXPLAINED
\i e11.sql
\! date -Iseconds
\! echo Q11 RUN 1
\i q11.sql
\! echo Q11 RUN 2
\i q11.sql
\! echo Q11 RUN 3
\i q11.sql
\! echo
\! echo Q15 EXPLAINED
\i e15.sql
\! date -Iseconds
\! echo Q15 RUN 1
\i q15.sql
\! echo Q15 RUN 2
\i q15.sql
\! echo Q15 RUN 3
\i q15.sql
\! echo
\! echo Q16 EXPLAINED
\i e16.sql
\! date -Iseconds
\! echo Q16 RUN 1
\i q16.sql
\! echo Q16 RUN 2
\i q16.sql
\! echo Q16 RUN 3
\i q16.sql
\! echo
\! echo Q17 EXPLAINED
\i e17.sql
\! date -Iseconds
\! echo Q17 RUN 1
\i q17.sql
\! echo Q17 RUN 2
\i q17.sql
\! echo Q17 RUN 3
\i q17.sql
\! echo
\! echo Q18 EXPLAINED
\i e19.sql
\! date -Iseconds
\! echo Q18 RUN 1
\i q18.sql
\! echo Q18 RUN 2
\i q18.sql
\! echo Q18 RUN 3
\i q18.sql
\! echo
\! echo Q19 EXPLAINED
\i e19.sql
\! date -Iseconds
\! echo Q19 RUN 1
\i q19.sql
\! echo Q19 RUN 2
\i q19.sql
\! echo Q19 RUN 3
\i q19.sql
\! echo
\! echo Q20 EXPLAINED
\i e20.sql
\! date -Iseconds
\! echo Q20 RUN 1
\i q20.sql
\! echo Q20 RUN 2
\i q20.sql
\! echo Q20 RUN 3
\i q20.sql
\! echo
\! echo DONE
\! date -Iseconds
