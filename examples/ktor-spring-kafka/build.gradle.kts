plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spotless)
  application
}

application {
  mainClass.set("io.github.osoykan.springkafka.example.ExampleKtorAppKt")
}

repositories {
  mavenCentral()
}

kotlin {
  compilerOptions {
    allWarningsAsErrors.set(true)
  }
}

spotless {
  kotlin {
    ktlint(
      libs.ktlint.cli
        .get()
        .version
    ).setEditorConfigPath(rootProject.layout.projectDirectory.file(".editorconfig"))
    targetExclude("build/", "generated/", "out/")
  }
  kotlinGradle {
    ktlint(
      libs.ktlint.cli
        .get()
        .version
    ).setEditorConfigPath(rootProject.layout.projectDirectory.file(".editorconfig"))
  }
}

dependencies {
  // Ktor Spring Kafka Plugin
  implementation(project(":ktor-spring-kafka-plugin"))

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

  // Jackson
  implementation(libs.jackson.module.kotlin)
  implementation(libs.jackson.datatype.jsr310)

  // Coroutines
  implementation(libs.kotlinx.coroutines.core)
  implementation(libs.kotlinx.coroutines.jdk8)
  implementation(libs.kotlinx.coroutines.reactor) // Required for Spring Kafka suspend support

  // Testing
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.stove.testing.e2e)
  testImplementation(libs.stove.testing.e2e.http)
  testImplementation(libs.stove.testing.e2e.kafka)
  testImplementation(libs.stove.ktor.testing.e2e)
}

tasks.test {
  useJUnitPlatform()
}
