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
    admin.deleteTopics(topics)
    admin.createTopics(topics.map { topic -> NewTopic(topic, 1, 1) })
}
