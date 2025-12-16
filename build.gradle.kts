plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.maven.publish)
  alias(libs.plugins.spotless)
  alias(libs.plugins.testlogger)
}

group = "io.github.osoykan"
version = "0.1.0"

repositories {
  mavenCentral()
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(21))
  }
}

kotlin {
  jvmToolchain(21)
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
    targetExcludeIfContentContains("generated")
    targetExcludeIfContentContainsRegex("generated.*")
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
  // Spring Kafka (default poller)
  api(libs.spring.kafka)

  // Reactor Kafka (optional alternative poller)
  compileOnly(libs.reactor.kafka)
  compileOnly(libs.kotlinx.coroutines.reactor)

  // Kotlin Coroutines
  api(libs.kotlinx.coroutines.core)
  api(libs.kotlinx.coroutines.jdk8)

  // Logging
  api(libs.kotlin.logging)

  // Test dependencies
  testImplementation(libs.logback.classic)
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.testcontainers.kafka)
  testImplementation(libs.spring.kafka.test)
  testImplementation(libs.reactor.kafka)
  testImplementation(libs.kotlinx.coroutines.reactor)
}

tasks.test {
  useJUnitPlatform()
}

testlogger {
  theme = com.adarshr.gradle.testlogger.theme.ThemeType.MOCHA
  showExceptions = true
  showStackTraces = true
  showFullStackTraces = false
  showCauses = true
  slowThreshold = 2000
  showSummary = true
  showSimpleNames = true
  showPassed = true
  showSkipped = true
  showFailed = true
}

mavenPublishing {
  coordinates(group.toString(), "kafka-flow", version.toString())

  pom {
    name.set("Kafka Flow")
    description.set("Kotlin Flow-based Kafka consumer/producer library built on Spring Kafka")
    url.set("https://github.com/osoykan/kafka-flow")
    inceptionYear.set("2025")

    licenses {
      license {
        name.set("Apache License, Version 2.0")
        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
      }
    }

    developers {
      developer {
        id.set("osoykan")
        name.set("Oguzhan Soykan")
        url.set("https://github.com/osoykan")
      }
    }

    scm {
      url.set("https://github.com/osoykan/kafka-flow")
      connection.set("scm:git:git://github.com/osoykan/kafka-flow.git")
      developerConnection.set("scm:git:ssh://git@github.com/osoykan/kafka-flow.git")
    }
  }
}
