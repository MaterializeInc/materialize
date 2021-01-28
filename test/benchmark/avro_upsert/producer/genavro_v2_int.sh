#! /bin/bash
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Generates about 3M unique values with 100 + 32000 (for bookId plus SecurityId)

set -euo pipefail

for _ in {1..400000000} ## 400M rows
do
    bookID=$((1+RANDOM % 100))
    SecurityID=$((1+RANDOM % 32000))
    Exposure1=$((SecurityID+bookID+100000+RANDOM))
    Exposure2=$((Exposure1+RANDOM))
    Exposure3=$((Exposure1+RANDOM))
    Exposure4=$((Exposure1+RANDOM))
    #bookID=`shuf -i 1-9 -n 1 --random-source=/dev/urandom`
    #SecurityID=$((bookID+1000))
    #SecurityID=`shuf -i 1-1000 -n 1 --random-source=/dev/urandom`
    #Exposure1=`shuf -i 1-100000 -n 1 --random-source=/dev/urandom`
    #Exposure2=`shuf -i 1-100000 -n 1 --random-source=/dev/urandom`
    #Exposure3=`shuf -i 1-100000 -n 1 --random-source=/dev/urandom`
    #Exposure4=`shuf -i 1-100000 -n 1 --random-source=/dev/urandom`

    #echo $bookID
    #echo $SecurityID
    echo "$bookID"'-'"$SecurityID"':'"{\"BookId\": $bookID, \"SecurityId\": $SecurityID, \"Exposure\": {\"Current\": {\"Long2\": {\"Exposure\": $Exposure1 }, \"Short2\": {\"Exposure\": $Exposure2}}, \"Target\": {\"Long\": {\"Exposure\": $Exposure3}, \"Short\": {\"Exposure\": $Exposure4}}}}"
done
