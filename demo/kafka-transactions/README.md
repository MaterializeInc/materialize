1. To start the demo, ensure that materialized and confluent are running

2. Compile the demo by running mvn package.

The demo is configurable in different ways. To see the different options, run
java -jar target/kafka-transactions-1.0-SNAPSHOT.jar

Example: The following call will create 4 concurrent producers, that will each execute 10,000 total operations. Operations will be split into transactions that will abort 5% of the time. Each transaction will contain between 0 and 5 writes per partition.

java -jar target/kafka-transactions-1.0-SNAPSHOT.jar  --topic-name=test-topic1,test-topic2 -j 1 -p 2 -o 10000 -w 5 -c 4 -a 5

3. In a materialize shell (psql -h localhost -p 6875 sslmode=disable -d materialize)

first create a source per specified topic:

> create materialized source s1 from kafka broker 'localhost:9092' topic 'test-topic1' format text

> create materialized source s1 from kafka broker 'localhost:9092' topic 'test-topic2' format text

The output of the query (modified to match the number of sources), should match the output of the benchmark "Finished loading data. Expected result: X  operations"

> select a+b from (select count(*) as a from s1),(select count(*) as b from s2)
