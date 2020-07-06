#!/bin/bash
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-2-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-3-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input-weekly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input-monthly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-2-input-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-2-input-weekly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-3-input-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-3-input-weekly
