package io.bsamartins.sandbox.kafka.kstreams.aggregate

import io.bsamartins.sandbox.kafka.kstreams.Contact
import io.bsamartins.sandbox.kafka.kstreams.Record
import io.bsamartins.sandbox.kafka.kstreams.Topic
import io.bsamartins.sandbox.kafka.kstreams.User
import io.bsamartins.sandbox.kafka.kstreams.createProducer
import io.bsamartins.sandbox.kafka.kstreams.sendRecord
import org.apache.logging.log4j.LogManager
import java.util.*

fun main() {
    val logger = LogManager.getLogger("producer")
    logger.info("Running producer")

    val producer = createProducer()

    val user = User(id = UUID.randomUUID().toString(), "test")

    val emailContact = Contact(id = UUID.randomUUID().toString(), userId = user.id, contact = "test@mail.com")
    val phoneContact = Contact(id = UUID.randomUUID().toString(), userId = user.id, contact = "555-5555-5555")

    producer.sendRecord(Topic.USER_TOPIC, Record.add(user.id, user))
    Thread.sleep(10000)

    producer.sendRecord(Topic.USER_CONTACT_TOPIC, Record.add(emailContact.id, emailContact)) // Add
    Thread.sleep(10000)

    producer.sendRecord(Topic.USER_CONTACT_TOPIC, Record.add(phoneContact.id, phoneContact)) // Add
    Thread.sleep(10000)

    producer.sendRecord(Topic.USER_CONTACT_TOPIC, Record.update(emailContact.id, emailContact, emailContact.copy(contact = "tom.cruise@hollywood.com"))) // Update
    Thread.sleep(10000)

    producer.sendRecord(Topic.USER_CONTACT_TOPIC, Record.delete<Any>(phoneContact.id, phoneContact)) // Delete
    Thread.sleep(10000)
}
