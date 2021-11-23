
subprojects {
    apply<com.github.davidmc24.gradle.plugin.avro.AvroPlugin>()

    dependencies {
        implementation("org.apache.avro:avro")
        implementation("org.apache.kafka:kafka-clients")
        implementation("io.confluent:kafka-avro-serializer")
    }
}

