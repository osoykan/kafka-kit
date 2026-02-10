@file:Suppress("UnstableApiUsage")

import dev.aga.gradle.versioncatalogs.Generator.generate
import dev.aga.gradle.versioncatalogs.GeneratorConfig

rootProject.name = "kafka-kit"
include(
  "kafka-flow",
  "ktor-kafka",
  "examples:ktor-kafka-flow",
  "examples:ktor-spring-kafka",
  "examples:shared"
)

plugins {
  id("dev.aga.gradle.version-catalog-generator") version ("4.0.0")
}
dependencyResolutionManagement {
  repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots") {
      content {
        includeGroup("com.trendyol")
      }
    }
  }
  versionCatalogs {
    generate("stoveLibs") {
      fromToml("stove-bom") {
        aliasPrefixGenerator = GeneratorConfig.NO_PREFIX
      }
    }
  }
}
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
