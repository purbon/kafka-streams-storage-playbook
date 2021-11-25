#!/usr/bin/env bash


kafka-avro-console-consumer --topic orders-avro  --bootstrap-server broker:9092  --from-beginning
