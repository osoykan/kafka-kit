plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spotless)
  application
}

application {
  mainClass.set("io.github.osoykan.kafkaflow.example.ExampleKtorAppKt")
}

repositories {
  mavenCentral()
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
  // Kafka Flow
  implementation(project(":"))

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
}
