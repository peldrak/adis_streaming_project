# adis_streaming_project

## Run Confluent Platform

```
export CONFLUENT_HOME=/home/peldrak/Desktop/adis_streaming_project/confluent-7.3.1
export PATH=$CONFLUENT_HOME/bin:$PATH
confluent local services start
```

## Add schema to Schema-Registry
Execute the following command for every topic:

```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data
'{"schema": "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\",
\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"},
{\"name\": \"value\", \"type\": \"float\"}]}"}' http://localhost:8081/subjects/[topic_name]-value/versions
```

## Submit the consumer.py in spark

```
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.spark:spark-avro_2.12:3.3.2  path_to/consumer.py
```

## Run producer.py

```
python3 path_to/producer.py
```
