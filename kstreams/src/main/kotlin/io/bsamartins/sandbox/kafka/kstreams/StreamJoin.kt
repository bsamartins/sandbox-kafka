package io.bsamartins.sandbox.kafka.kstreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.StreamJoined
import java.time.Duration
import java.util.*

fun main() {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "stream-inner-join"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java

    val builder = StreamsBuilder()

    val leftSource = builder.stream<String, String>(Topic.USER_TOPIC)
    val rightSource = builder.stream<String, String>(Topic.USER_CONTACT_TOPIC)

    val joined: KStream<String, String> = leftSource.join(
        rightSource,
        { leftValue: String, rightValue: String -> "left=$leftValue, right=$rightValue" },
        JoinWindows.of(Duration.ofMinutes(5)),
        StreamJoined.with(
            Serdes.String(),
            Serdes.String(),
            Serdes.String(),
        )
    )

    joined.to("io.bsamartins.kstream.user-join")

    val topology = builder.build()
    val streamsInnerJoin = KafkaStreams(topology, props)
    streamsInnerJoin.start()
}
