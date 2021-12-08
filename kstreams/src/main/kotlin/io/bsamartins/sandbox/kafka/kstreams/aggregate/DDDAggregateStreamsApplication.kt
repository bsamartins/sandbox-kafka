package io.bsamartins.sandbox.kafka.kstreams.aggregate

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.bsamartins.sandbox.kafka.kstreams.Contact
import io.bsamartins.sandbox.kafka.kstreams.Record
import io.bsamartins.sandbox.kafka.kstreams.Topic
import io.bsamartins.sandbox.kafka.kstreams.User
import io.bsamartins.sandbox.kafka.kstreams.jsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.logging.log4j.LogManager
import java.time.Duration
import java.util.*

fun main() {
    val logger = LogManager.getLogger("streams")

    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "io.bsamartins.ddd-aggregate"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    props[StreamsConfig.POLL_MS_CONFIG] = Duration.ofSeconds(1).toMillis()

    val builder = StreamsBuilder()

    val defaultIdSerde = Serdes.Integer()
    val userSerde = jsonSerde<Record<User>>()
    val contactSerde = jsonSerde<Record<Contact>>()
    val contactsSerde = jsonSerde<Contacts>()
    val latestContactSerde = jsonSerde<LatestContact>()
    val aggregateSerde = jsonSerde<UserAggregate>()

    val userTable = builder.table(Topic.USER_TOPIC, Consumed.with(defaultIdSerde, userSerde))
    val contactStream = builder.stream(Topic.USER_CONTACT_TOPIC, Consumed.with(defaultIdSerde, contactSerde))

    val contactTempTable = contactStream
        .groupByKey(Grouped.with(defaultIdSerde, contactSerde))
        .aggregate(
            { null },
            { contactId, contactRecord, latest: LatestContact? ->
                logger.info("contact: key={}, contact={}, latest={}", contactId, contactRecord, latest)
                val userId = (contactRecord.after ?: contactRecord.before)!!.userId
                LatestContact(id = contactId, userId = userId, latest = contactRecord.after)
            },
            Materialized.`as`<Int, LatestContact, KeyValueStore<Bytes, ByteArray>>(Topic.USER_CONTACT_TOPIC + "_table_temp")
                .withKeySerde(defaultIdSerde)
                .withValueSerde(latestContactSerde)
        )

    val contactTable = contactTempTable.toStream()
        .map { _, latestContact: LatestContact ->
            KeyValue(latestContact.userId, latestContact)
        }
        .groupByKey(Grouped.with(defaultIdSerde, latestContactSerde))
        .aggregate(
            { Contacts() },
            { userId, latest, contacts: Contacts ->
                logger.info("contacts: userId={}, latest={}, contacts={}", userId, latest, contacts)
                contacts.update(latest)
                contacts
            },
            Materialized.`as`<Int, Contacts, KeyValueStore<Bytes, ByteArray>>(Topic.USER_CONTACT_TOPIC + "_table_aggregate")
                .withKeySerde(defaultIdSerde)
                .withValueSerde(contactsSerde)
        )

    val dddAggregate = userTable.join(contactTable) { record, contacts ->
        logger.info("aggregate")
        val user = record.after
        if (user == null) null
        else UserAggregate(user, contacts.getEntries())
    }

    dddAggregate.toStream().to(Topic.USER_AGGREGATE_TOPIC, Produced.with(defaultIdSerde, aggregateSerde))

    val topology = builder.build()
    val streamsInnerJoin = KafkaStreams(topology, props)
    streamsInnerJoin.start()
    logger.info("Started streams")
}

data class LatestContact(val id: Int, val userId: Int, val latest: Contact?)

class Contacts {
    @JsonProperty
    private val entries: MutableMap<Int, Contact> = mutableMapOf()

    fun update(contact: LatestContact) {
        if (contact.latest != null) {
            entries[contact.id] = contact.latest
        } else {
            entries.remove(contact.id)
        }
    }

    @JsonIgnore
    fun getEntries(): List<Contact> {
        return entries.values.toList()
    }

    override fun toString(): String {
        return "Contacts{" +
            "entries=" + entries +
            "}"
    }
}

data class UserAggregate @JsonCreator constructor(
    @JsonProperty("user") val user: User,
    @JsonProperty("contacts") val contacts: List<Contact>
)
