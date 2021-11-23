
subprojects {
    apply<com.github.davidmc24.gradle.plugin.avro.AvroPlugin>()

    dependencies {
        implementation("org.apache.kafka:kafka-clients")
        implementation("io.apicurio:apicurio-registry-serdes-avro-serde")
    }
}

