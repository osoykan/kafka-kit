plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spotless)
  `java-library`
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

tasks.test {
  useJUnitPlatform()
}
