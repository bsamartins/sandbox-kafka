package io.bsamartins.sandbox.kafka.kstreams

import java.util.*

fun main() {
    val producer = createProducer()
    val user = User(
        id = UUID.randomUUID().toString(),
        name = "Tom Cruise",
    )
    val contact1 = Contact(
        id = UUID.randomUUID().toString(),
        userId = user.id,
        contact = "t.cruise@mail.com"
    )
    val contact2 = Contact(
        id = UUID.randomUUID().toString(),
        userId = user.id,
        contact = "555-5555"
    )
    producer.send(Topic.USER_TOPIC, user.id, user).get()
    producer.send(Topic.USER_CONTACT_TOPIC, contact1.id, contact1).get()
    producer.send(Topic.USER_CONTACT_TOPIC, contact2.id, contact2).get()
}

data class User(val id: String, val name: String)
data class Contact(val id: String, val userId: String, val contact: String)
