plugins {
  `java-library`
}

dependencies {
  // Ktor
  api(libs.ktor.server.core)

  // Spring Kafka
  api(libs.spring.kafka)

  // Coroutines (reactor required for Spring Kafka suspend support)
  implementation(libs.kotlinx.coroutines.core)
  implementation(libs.kotlinx.coroutines.reactor)

  // Logging
  implementation(libs.kotlin.logging)

  // Testing
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.ktor.server.test.host)
}
