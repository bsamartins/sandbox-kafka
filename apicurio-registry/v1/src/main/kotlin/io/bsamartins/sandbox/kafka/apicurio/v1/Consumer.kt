package io.bsamartins.sandbox.kafka.apicurio.v1

import io.apicurio.registry.rest.client.config.ClientConfig
import io.apicurio.registry.serde.SerdeConfig
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import io.bsamartins.schema.User
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties

fun main() {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ConsumerConfig.GROUP_ID_CONFIG] = "group-v1"
    props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "true"
    props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "1000"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    props[SerdeConfig.REGISTRY_URL] = "http://localhost:8081"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = AvroKafkaDeserializer::class.java

    val consumer = KafkaConsumer<String, User>(props)
    consumer.subscribe(listOf(TOPIC))

    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            println("key=$${record.key()}, value=${record.value()}")
        }
    }
}
