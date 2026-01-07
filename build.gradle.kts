plugins {
  alias(libs.plugins.kotlin.jvm) apply false
  alias(libs.plugins.maven.publish) apply false
  alias(libs.plugins.spotless)
  alias(libs.plugins.testlogger) apply false
}

group = "io.github.osoykan"
version = "0.1.0"

allprojects {
  repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots") {
      content {
        includeGroup("com.trendyol")
      }
    }
  }
}

subprojects {
  apply(plugin = "org.jetbrains.kotlin.jvm")
  apply(plugin = "com.diffplug.spotless")
  apply(plugin = "com.adarshr.test-logger")

  configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
    jvmToolchain(21)
    compilerOptions {
      allWarningsAsErrors.set(true)
    }
  }

  configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    kotlin {
      ktlint(
        rootProject.libs.ktlint.cli
          .get()
          .version
      ).setEditorConfigPath(rootProject.layout.projectDirectory.file(".editorconfig"))
      targetExclude("build/", "generated/", "out/")
      targetExcludeIfContentContains("generated")
      targetExcludeIfContentContainsRegex("generated.*")
    }
    kotlinGradle {
      ktlint(
        rootProject.libs.ktlint.cli
          .get()
          .version
      ).setEditorConfigPath(rootProject.layout.projectDirectory.file(".editorconfig"))
    }
  }

  configure<com.adarshr.gradle.testlogger.TestLoggerExtension> {
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

  tasks.withType<Test> {
    useJUnitPlatform()
  }
}

// Root project spotless for build.gradle.kts files
spotless {
  kotlinGradle {
    ktlint(
      libs.ktlint.cli
        .get()
        .version
    ).setEditorConfigPath(layout.projectDirectory.file(".editorconfig"))
  }
}
