sudo apt-get update
sudo apt-get install -qy default-jre postgresql-client

curl -fL http://packages.confluent.io/archive/7.3/confluent-community-7.3.0.tar.gz > confluent.tar.gz
mkdir confluent
(cd confluent && tar --strip-components=1 -xf ../confluent.tar.gz)
rm confluent.tar.gz

cat > kafka.properties <<EOF
security.protocol=SSL
EOF

confluent/bin/kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --command-config kafka.properties \
  --create \
  --topic test
confluent/bin/kafka-avro-console-producer \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --producer.config kafka.properties \
  --topic test \
  --property value.schema='{"type":"record","name":"test","fields":[{"name":"a","type":"int"}]}' \
  --property schema.registry.url=$SCHEMA_REGISTRY_URL <<EOF
{"a": 1}
{"a": 2}
EOF
