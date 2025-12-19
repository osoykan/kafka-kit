plugins {
  application
}

application {
  mainClass.set("io.github.osoykan.springkafka.example.ExampleKtorAppKt")
}

dependencies {
  // Ktor Kafka Plugin
  implementation(project(":ktor-kafka"))

  // Ktor Server
  implementation(libs.ktor.server.core)
  implementation(libs.ktor.server.netty)
  implementation(libs.ktor.server.content.negotiation)
  implementation(libs.ktor.server.status.pages)
  implementation(libs.ktor.server.auto.head.response)
  implementation(libs.ktor.serialization.jackson)

  // Koin DI
  implementation(libs.koin.core)
  implementation(libs.koin.ktor)

  // Configuration
  implementation(libs.hoplite.core)
  implementation(libs.hoplite.yaml)

  // Logging
  implementation(libs.kotlin.logging)
  implementation(libs.logback.classic)

  // Coroutines
  implementation(libs.kotlinx.coroutines.core)
  implementation(libs.kotlinx.coroutines.jdk8)
  implementation(libs.kotlinx.coroutines.reactor) // Required for Spring Kafka suspend support
  implementation(projects.examples.shared)
  implementation(libs.jackson3.kotlin)

  // Testing
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.stove.testing.e2e)
  testImplementation(libs.stove.testing.e2e.http)
  testImplementation(libs.stove.testing.e2e.kafka)
  testImplementation(libs.stove.ktor.testing.e2e)
}
