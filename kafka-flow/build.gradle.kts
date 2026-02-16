plugins {
  alias(libs.plugins.maven.publish)
}

dependencies {
  // Spring Kafka
  api(libs.spring.kafka)

  // Kotlin Coroutines
  implementation(libs.kotlinx.coroutines.core)
  implementation(libs.kotlinx.coroutines.jdk8)

  // Logging
  implementation(libs.kotlin.logging)

  // Test dependencies
  testImplementation(libs.logback.classic)
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
  testImplementation(libs.testcontainers.kafka)
  testImplementation(libs.spring.kafka.test)
}

sourceSets {
  create("testIntegration") {
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
  }
}

configurations["testIntegrationImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["testIntegrationRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("testIntegration") {
  description = "Runs integration tests"
  group = "verification"
  testClassesDirs = sourceSets["testIntegration"].output.classesDirs
  classpath = sourceSets["testIntegration"].runtimeClasspath
  useJUnitPlatform()
  shouldRunAfter(tasks.test)
}

tasks.check {
  dependsOn(tasks.named("testIntegration"))
}

mavenPublishing {
  coordinates("io.github.osoykan", "kafka-flow", version.toString())

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
