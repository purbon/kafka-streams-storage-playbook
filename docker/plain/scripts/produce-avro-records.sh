#!/usr/bin/env bash


#kafka-avro-console-producer --topic example-topic-avro --bootstrap-server broker:9092 --property value.schema="$(< /opt/app/schema/order_detail.avsc)" < /opt/app/schema/events.txt

kafka-avro-console-producer --topic orders-avro \
          --bootstrap-server broker:9092 \
          --property key.schema="$(< /opt/app/schema/tx.avsc)" \
          --property value.schema="$(< /opt/app/schema/order_detail.avsc)" \
          --property parse.key=true \
          --property schema.registry.url=http://schema-registry:8081 \
          --property key.separator="#" < /opt/app/schema/events.txt

kafka-avro-console-producer --topic more-orders-avro \
          --bootstrap-server broker:9092 \
          --property key.schema="$(< /opt/app/schema/other_tx.avsc)" \
          --property value.schema="$(< /opt/app/schema/other_order.avsc)" \
          --property parse.key=true \
          --property schema.registry.url=http://schema-registry:8081 \
          --property key.separator="#" < /opt/app/schema/other_events.txt


kafka-avro-console-producer --topic TransactionMain \
          --bootstrap-server broker:9092 \
          --property key.schema="$(< /opt/app/schema/tx/tx_key.avsc)" \
          --property value.schema="$(< /opt/app/schema/tx/tx_detail.avsc)" \
          --property parse.key=true \
          --property schema.registry.url=http://schema-registry:8081 \
          --property key.separator="#" < /opt/app/schema/tx/tx_events.txt

kafka-avro-console-producer --topic TransactionMain \
          --bootstrap-server broker:9092 \
          --property key.schema="$(< /opt/app/schema/tx/tx_key.avsc)" \
          --property value.schema="$(< /opt/app/schema/tx/tx_detail.avsc)" \
          --property parse.key=true \
          --property schema.registry.url=http://schema-registry:8081 \
          --property key.separator="#" < /opt/app/schema/tx/tx_events_new.txt

kafka-avro-console-producer --topic CreditCardInfo \
          --bootstrap-server broker:9092 \
          --property key.schema="$(< /opt/app/schema/tx/cci_key.avsc)" \
          --property value.schema="$(< /opt/app/schema/tx/cci_detail.avsc)" \
          --property parse.key=true \
          --property schema.registry.url=http://schema-registry:8081 \
          --property key.separator="#" < /opt/app/schema/tx/cci_events.txt
