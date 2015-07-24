# python-kafka-elasticsearch
## Simple learning project pushing CSV data into Kafka then indexing the data in ElasticSearch.

The indexes are created with a very rudimentary type discovery that uses simple regex patterns.

The Sacramento crime January 2006 dataset contains 7,584 crime records, as made available by the Sacramento Police Department.

### You need the following components installed:

* ElasticSearch, 1.8+
* Kafka, 2.10+
* Kibana, 4.1.1+
* Python 2.7+

### To install necessary python libraries:

* sudo pip install -r requirements.txt

### To startup Kafka:

* untar the distribution & cd kafka_2.10-0.8.2.1
* start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
* start kafka: bin/kafka-server-start.sh config/server.properties
* create a topic: bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

See the [Kafka QuickStart Guide ](http://kafka.apache.org/documentation.html#quickstart)
