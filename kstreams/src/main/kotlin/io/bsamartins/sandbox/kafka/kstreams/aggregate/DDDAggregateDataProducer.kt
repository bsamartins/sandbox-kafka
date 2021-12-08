package io.bsamartins.sandbox.kafka.kstreams.aggregate

import io.bsamartins.sandbox.kafka.kstreams.Contact
import io.bsamartins.sandbox.kafka.kstreams.ContactSequence
import io.bsamartins.sandbox.kafka.kstreams.Record
import io.bsamartins.sandbox.kafka.kstreams.Topic
import io.bsamartins.sandbox.kafka.kstreams.User
import io.bsamartins.sandbox.kafka.kstreams.UserSequence
import io.bsamartins.sandbox.kafka.kstreams.createProducer
import io.bsamartins.sandbox.kafka.kstreams.sendRecord
import org.apache.logging.log4j.LogManager

fun main() {
    val logger = LogManager.getLogger("producer")
    logger.info("Running producer")

    val producer = createProducer()

    val user = User(id = UserSequence.next(), "test")

    val emailContact = Contact(id = ContactSequence.next(), userId = user.id, contact = "test@mail.com")
    val phoneContact = Contact(id = ContactSequence.next(), userId = user.id, contact = "555-5555-5555")

    producer.sendRecord(topic = Topic.USER_TOPIC, record = Record.add(user.id, user))
    producer.sendRecord(topic = Topic.USER_CONTACT_TOPIC, partition = 1, record = Record.add(emailContact.id, emailContact)) // Add
    producer.sendRecord(topic = Topic.USER_CONTACT_TOPIC, record = Record.add(phoneContact.id, phoneContact)) // Add
    producer.sendRecord(topic = Topic.USER_CONTACT_TOPIC, partition = 1, record = Record.update(emailContact.id, emailContact, emailContact.copy(contact = "tom.cruise@hollywood.com"))) // Update
    producer.sendRecord(topic = Topic.USER_CONTACT_TOPIC, record = Record.delete<Any>(phoneContact.id, phoneContact)) // Delete
}
