package io.github.osoykan.kafka.toolkit

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

fun dumpThreadInfo(from: String) {
  val thread = Thread.currentThread()
  buildString {
    appendLine("=== Threading Info Dump ($from) ===")
    appendLine("Current Thread: ${thread.name} (ID: ${thread.threadId()})")
    appendLine("Is Daemon: ${thread.isDaemon}")
    appendLine("Is Virtual: ${thread.isVirtual}")
    appendLine("Priority: ${thread.priority}")
    appendLine("Thread Group: ${thread.threadGroup?.name}")
    appendLine("=== End of Threading Info Dump ===")
  }.also { logger.info { it } }
}
