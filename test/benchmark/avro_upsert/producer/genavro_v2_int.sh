#! /bin/bash

# Generates about 3M unique values with 100 + 32000 (for bookId plus SecurityId)
# Average of 25K rows/sec output
# bin/kafka-avro-console-producer --property key.serializer=org.apache.kafka.common.serialization.StringSerializer     --broker-list compute2:9092 --property schema.registry.url=http://compute2:8081    --topic upsertavrotest3   --property parse.key=true --property key.schema='{"type" : "string"}' --property "key.separator=:"     --property value.schema='{"name": "upsertavrotest", "type": "record", "namespace": "com.acme.avro", "fields": [{"name": "BookId", "type": "long"}, {"name": "SecurityId", "type": "long"}, {"name": "Exposure", "type": {"name": "Exposure", "type": "record", "fields": [{"name": "Current", "type": {"name": "Current", "type": "record", "fields": [{"name": "Long2", "type": {"name": "Long2", "type": "record", "fields": [{"name": "Exposure", "type": "string"} ] } }, {"name": "Short2", "type": {"name": "Short2", "type": "record", "fields": [{"name": "Exposure", "type": "string"} ] } } ] } }, {"name": "Target", "type": {"name": "Target", "type": "record", "fields": [{"name": "Long", "type": {"name": "Long", "type": "record", "fields": [{"name": "Exposure", "type": "string"} ] } }, {"name": "Short", "type": {"name": "Short", "type": "record", "fields": [{"name": "Exposure", "type": "string"} ] } } ] } } ] } } ] }'


for i in {1..10000000} ## 10M rows
do
bookID=$((1+$RANDOM % 100))
SecurityID=$((1+$RANDOM % 32000))
Exposure1=$((SecurityID+bookID+100000+$RANDOM))
Exposure2=$((Exposure1+$RANDOM))
Exposure3=$((Exposure1+$RANDOM))
Exposure4=$((Exposure1+$RANDOM))
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
