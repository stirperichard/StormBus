#!/bin/bash
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-1-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-2-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-3-input
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-1-output-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-1-output-weekly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-1-output-monthly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-2-output-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-2-output-weekly
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-3-output-daily
docker exec --user=root kafka0 kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query-3-output-weekly