#!/bin/zsh

brew services start kafka 
kafka-server-start kafka-servers/server1.properties &
kafka-server-start kafka-servers/server2.properties &
kafka-server-start kafka-servers/server3.properties &
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic messagingQueue --if-not-exists