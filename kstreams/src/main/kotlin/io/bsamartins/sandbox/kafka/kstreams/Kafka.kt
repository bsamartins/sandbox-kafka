package io.bsamartins.sandbox.kafka.kstreams

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.IntegerSerializer
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
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java.name
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    return props
}

fun createProducer(): Producer<Int, String> {
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

fun Producer<Int, String>.send(topic: String, partition: Int? = null, key: Int, value: Any?): Future<RecordMetadata> {
    val record = ProducerRecord(
        topic,
        partition,
        key,
        OBJECT_MAPPER.writeValueAsString(value)
    )
    return this.send(record)
}

fun <T> Producer<Int, String>.sendRecord(topic: String, partition: Int? = null, record: Record<T>): Future<RecordMetadata> {
    return this.send(topic = topic, key = record.id, value = record, partition = partition)
}
