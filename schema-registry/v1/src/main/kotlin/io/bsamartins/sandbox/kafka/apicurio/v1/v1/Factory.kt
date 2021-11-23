package io.bsamartins.sandbox.kafka.apicurio.v1.v1

import io.bsamartins.schema.User
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun getProducerProperties(): Properties {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "http://localhost:9092"
    props[ProducerConfig.CLIENT_ID_CONFIG] = "io.bsamartins.schema-registry.p1"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.name
    props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
    return props
}

fun createProducer(): Producer<String, User> {
    val props = getProducerProperties()
    return KafkaProducer(props)
}
