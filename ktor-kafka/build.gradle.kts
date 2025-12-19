plugins {
  alias(libs.plugins.maven.publish)
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

mavenPublishing {
  coordinates("io.github.osoykan", "ktor-kafka", version.toString())

  pom {
    name.set("Ktor Kafka")
    description.set("Ktor plugin for Kafka integration with Spring Kafka")
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
