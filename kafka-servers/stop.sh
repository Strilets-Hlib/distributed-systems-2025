#!/bin/zsh

kafka-topics --delete --topic messagingQueue --bootstrap-server localhost:9092
brew services stop kafka