#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# generate-batch-update — generates batch updates (inserts/deletes) from LDBC BI csv files

set -eu -o pipefail

: "${csvs=bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk}"

START=2012-11-29
END=2012-12-31 # was 2013-01-01 exclusive

[[ "$#" -eq 1 ]] || {
    printf "Usage: %s [BATCH IN RANGE %s TO %s INCLUSIVE]\n" "$(basename "$0")" "$START" "$END" >&2
    exit 1
}

case "$1" in
    (2012-11-29|2012-11-30|2012-12-0[1-9]|2012-12-[12][0-9]|2012-12-3[01]);;
    (*) printf "%s: %s is not a valid date between %s and %s (inclusive)\n" "$(basename "$0")" "$1" "$START" "$END" >&2
    exit 1
esac

batch_id="batch_id=$1"
#exec 1>"$batch_id.sql"

# inserts
printf -- "-- INSERTS\n\n"

for entity in Comment Comment_hasTag_Tag Forum Forum_hasMember_Person Forum_hasTag_Tag Person Person_hasInterest_Tag Person_knows_Person Person_studyAt_University Person_workAt_Company Person_likes_Comment Person_likes_Post Post Post_hasTag_Tag
do
    for csv in "$csvs/inserts/dynamic/$entity/$batch_id/part-"*.csv
    do
        printf "\\copy %s FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"

	if [ "$entity" = "Person_knows_Person" ]
	then
            printf -- "-- make it symmetric\n"
	    printf "\\copy %s (creationDate, Person2id, Person1id) FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"
	fi
    done
done

# Umbra has some limitations that means they can't use a direct
# `DELETE USING` if a deletion candidate shows up more than once. We
# have no such limitation, which should make things easier.
#
# We'll still need to manually delete threads with deleted roots, though.

# deletes
printf -- "\n-- DELETES\n\n"

for entity in Comment Post Forum Person Forum_hasMember_Person Person_knows_Person Person_likes_Comment Person_likes_Post
do
    printf "DELETE FROM %s_Delete_candidates;\n" "$entity"

    for csv in  "$csvs/deletes/dynamic/$entity/$batch_id/part-"*.csv
    do

        printf "\\copy %s_Delete_candidates FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"

        # we don't symmetrize Person_knows_Person deletions---we'll do it in the deletes.sql
    done
done

printf -- "\n-- APPLY DELETIONS\n"

grep -ve '^#' deletes.sql
