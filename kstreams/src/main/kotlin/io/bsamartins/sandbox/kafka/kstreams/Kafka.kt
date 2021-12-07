package io.bsamartins.sandbox.kafka.kstreams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.Future

private val OBJECT_MAPPER = jacksonObjectMapper()
val producerProperties = buildProducerProperties()
val adminProperties = buildAdminProperties()

private fun buildProducerProperties(): Properties {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "http://localhost:9092"
    props[ProducerConfig.CLIENT_ID_CONFIG] = "io.bsamartins.kstreams.data-producer"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    return props
}

fun createProducer(): Producer<String, String> {
    return KafkaProducer(producerProperties)
}

fun buildAdminProperties(): Properties {
    val adminProperties = Properties()
    adminProperties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "http://localhost:9092"
    return adminProperties
}

fun createAdminClient(): AdminClient {
    return AdminClient.create(adminProperties)
}

fun Producer<String, String>.send(topic: String, id: String, value: Any?): Future<RecordMetadata> {
    val record = ProducerRecord(
        topic,
        id,
        OBJECT_MAPPER.writeValueAsString(value)
    )
    return this.send(record)
}

fun <T> Producer<String, String>.sendRecord(topic: String, record: Record<T>): Future<RecordMetadata> {
    return this.send(topic, record.id, record)
}
