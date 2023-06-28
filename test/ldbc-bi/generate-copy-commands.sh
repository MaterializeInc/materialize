#!/bin/sh

set -eu -o pipefail

: ${csvs=bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk}

exec 1>load.sql

printf -- "-- static entitities\n"
for entity in Organisation Place Tag TagClass	      
do
    for csv in "$csvs"/initial_snapshot/static/$entity/part-*.csv
    do
	printf "\\copy %s FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"
    done
done

printf -- "\n-- dynamic entitites\n"
for entity in Comment Forum Forum_hasMember_Person Forum_hasTag_Tag Person Person_hasInterest_Tag Person_knows_Person Person_studyAt_University Person_workAt_Company Post
do
    for csv in "$csvs"/initial_snapshot/dynamic/$entity/part-*.csv
    do
	printf "\\copy %s FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"

	# make it symmetric
	if [ "$entity" = "Person_knows_Person" ]
	then
	    printf "\\copy %s (creationDate, Person2id, Person1id) FROM '%s' WITH (DELIMITER '|', HEADER, NULL '', FORMAT CSV);\n" "$entity" "$csv"	    
	fi
    done

done

