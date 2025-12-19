dependencies {
  // Kotlin Coroutines
  implementation(libs.kotlinx.coroutines.core)

  // Logging
  implementation(libs.kotlin.logging)

  // Test dependencies
  testImplementation(libs.logback.classic)
  testImplementation(libs.kotest.runner.junit5)
  testImplementation(libs.kotest.assertions.core)
}
