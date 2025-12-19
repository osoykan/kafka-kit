rootProject.name = "kafka-kit"
include(
  "kafka-flow",
  "ktor-kafka",
  "examples:ktor-kafka-flow",
  "examples:ktor-spring-kafka",
  "examples:shared"
)
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
