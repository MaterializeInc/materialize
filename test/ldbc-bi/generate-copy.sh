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
# generate-copy — generates \copy commands from LDBC BI csv files

set -eu -o pipefail

: "${csvs=bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk}"

exec 1>copy.sql

printf -- "-- static entitities\n"
for entity in Organisation Place Tag TagClass
do
    for csv in "$csvs/initial_snapshot/static/$entity/part-"*.csv
    do
	printf "\\copy %s FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"
    done
done

printf -- "\n-- dynamic entitites\n"
for entity in Comment Comment_hasTag_Tag Forum Forum_hasMember_Person Forum_hasTag_Tag Person Person_hasInterest_Tag Person_knows_Person Person_studyAt_University Person_workAt_Company Person_likes_Comment Person_likes_Post Post Post_hasTag_Tag
do
    for csv in "$csvs/initial_snapshot/dynamic/$entity/part-"*.csv
    do
	printf "\\copy %s FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"

	# make it symmetric
	if [ "$entity" = "Person_knows_Person" ]
	then
	    printf "\\copy %s (creationDate, Person2id, Person1id) FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"
	fi
    done

done
