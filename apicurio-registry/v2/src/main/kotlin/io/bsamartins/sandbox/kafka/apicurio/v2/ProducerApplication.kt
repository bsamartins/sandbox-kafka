package io.bsamartins.sandbox.kafka.apicurio.v2

import io.bsamartins.schema.User
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.UUID

const val TOPIC = "io.bsamartins.schema-registry.user"

fun main() {
    val producer = createProducer()
    val record = ProducerRecord(
        TOPIC,
        UUID.randomUUID().toString(),
        User.newBuilder()
            .setName("Tom Cruise")
            .setFavoriteColor("Red")
            .setFavoriteNumber(10)
            .setAge(59)
            .build()
    )
    producer.send(record).get()
}
