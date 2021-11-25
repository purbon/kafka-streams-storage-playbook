#!/usr/bin/env bash

docker exec schema-registry /opt/scripts/produce-avro-records.sh

docker exec schema-registry /opt/scripts/read-avro-records.sh
