package io.bsamartins.sandbox.kafka.apicurio.v1

import io.apicurio.registry.rest.client.config.ClientConfig
import io.apicurio.registry.serde.SerdeConfig
import io.bsamartins.schema.User
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

const val TOPIC = "io.bsamartins.schema-registry.user"

fun getProducerProperties(): Properties {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "http://localhost:9092"
    props[ProducerConfig.CLIENT_ID_CONFIG] = "io.bsamartins.schema-registry.p1"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = io.apicurio.registry.serde.avro.AvroKafkaSerializer::class.java.name
    props[SerdeConfig.REGISTRY_URL] = "http://localhost:8081/api"
    props[ClientConfig.REGISTRY_CLIENT_AUTO_BASE_PATH] = ""
    props[SerdeConfig.AUTO_REGISTER_ARTIFACT] = true
    return props
}

fun createProducer(): Producer<String, User> {
    val props = getProducerProperties()
    return KafkaProducer(props)
}

fun main() {
    val producer = createProducer()
    val record = ProducerRecord(
        TOPIC,
        UUID.randomUUID().toString(),
        User.newBuilder()
            .setName("Tom Cruise")
            .setFavoriteColor("Red")
            .setFavoriteNumber(10)
            .build()
    )
    producer.send(record).get()
}
