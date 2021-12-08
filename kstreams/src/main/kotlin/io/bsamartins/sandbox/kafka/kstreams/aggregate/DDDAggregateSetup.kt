package io.bsamartins.sandbox.kafka.kstreams.aggregate

import io.bsamartins.sandbox.kafka.kstreams.Topic
import io.bsamartins.sandbox.kafka.kstreams.createAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.logging.log4j.LogManager

fun main() {
    val logger = LogManager.getLogger("DDDAggregateTopicSetup")
    logger.info("Running setup")
    val admin = createAdminClient()
    val topics = setOf(Topic.USER_TOPIC, Topic.USER_CONTACT_TOPIC, Topic.USER_AGGREGATE_TOPIC)

    admin.listTopics()
        .names()
        .get()
        .filter { it.startsWith("io.bsamartins") }
        .let { admin.deleteTopics(it) }
        .all()
        .get()
    admin.createTopics(topics.map { topic -> NewTopic(topic, 2, 1) })
        .all()
        .get()
}
