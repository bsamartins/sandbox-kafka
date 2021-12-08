package io.bsamartins.sandbox.kafka.kstreams

fun main() {
    val producer = createProducer()
    val user = User(
        id = UserSequence.next(),
        name = "Tom Cruise",
    )
    val contact1 = Contact(
        id = ContactSequence.next(),
        userId = user.id,
        contact = "t.cruise@mail.com"
    )
    val contact2 = Contact(
        id = ContactSequence.next(),
        userId = user.id,
        contact = "555-5555"
    )
    producer.send(topic = Topic.USER_TOPIC, key = user.id, value = user).get()
    producer.send(topic = Topic.USER_CONTACT_TOPIC, key = contact1.id, value = contact1).get()
    producer.send(topic = Topic.USER_CONTACT_TOPIC, key = contact2.id, value = contact2).get()
}

open class Sequence {
    var sequence: Int = 0
    fun next(): Int {
        return sequence++
    }
}

object UserSequence : Sequence()
object ContactSequence : Sequence()

data class User(val id: Int, val name: String)
data class Contact(val id: Int, val userId: Int, val contact: String)
