package io.bsamartins.sandbox.kafka.apicurio.v1.v2

import io.bsamartins.schema.User
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.Properties

fun main() {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ConsumerConfig.GROUP_ID_CONFIG] = "group-v2"
    props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "true"
    props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "1000"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
    props[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true

    val consumer = KafkaConsumer<String, User>(props)
    consumer.subscribe(listOf(TOPIC))

    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            println("key=$${record.key()}, value=${record.value()}")
        }
    }
}
