#!/bin/bash
/Users/simone/kafka_2.11-2.2.0/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input
/Users/simone/kafka_2.11-2.2.0/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-output
