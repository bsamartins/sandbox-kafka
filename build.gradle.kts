plugins {
    kotlin("jvm") version "1.6.0" apply false
    id("com.github.davidmc24.gradle.plugin.avro") version "1.3.0" apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven")
    }

    dependencies {
        constraints {
            add("implementation", "org.apache.kafka:kafka-clients:3.0.0")
            add("implementation", "org.apache.avro:avro:1.11.0")
            add("implementation", "io.confluent:kafka-avro-serializer:7.0.0")
        }
    }
}
