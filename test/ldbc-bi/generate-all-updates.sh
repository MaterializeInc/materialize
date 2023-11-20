#!/usr/bin/env bash

for batch in 2012-11-29 2012-11-30 2012-12-0{1,2,3,4,5,6,7,8,9} 2012-12-{1,2}{0,1,2,3,4,5,6,7,8,9} 2012-12-3{0,1}
do
    ./generate-batch-update.sh "$batch" >batch_"$batch".sql
done
