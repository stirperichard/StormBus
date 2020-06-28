#!/bin/bash
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic t-singl-part
#docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic t-multi-part
