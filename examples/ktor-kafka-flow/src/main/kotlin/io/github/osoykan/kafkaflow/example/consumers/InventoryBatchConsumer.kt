package io.github.osoykan.kafkaflow.example.consumers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.osoykan.kafkaflow.*
import io.github.osoykan.kafkaflow.example.domain.InventoryEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

private val logger = KotlinLogging.logger {}

/**
 * Example batch consumer for inventory updates.
 *
 * Batch consumers receive all records from a single Kafka poll() as a list,
 * which is ideal for bulk operations like inventory updates where you can
 * persist many changes in a single database transaction.
 *
 * Key differences from per-record consumers:
 * - No automatic retry pipeline — you handle failures explicitly via [FailureHandler]
 * - The entire batch is acknowledged after [consume] returns (auto-ack)
 * - Records within a batch maintain partition ordering
 */
@KafkaTopic(
  name = "example.inventory",
  retry = "example.inventory.retry",
  dlt = "example.inventory.dlt"
)
class InventoryBatchConsumer : BatchConsumerAutoAck<String, InventoryEvent> {
  override suspend fun consume(
    records: List<ConsumerRecord<String, InventoryEvent>>,
    failureHandler: FailureHandler<String, InventoryEvent>
  ) {
    logger.info { "Received batch of ${records.size} inventory updates" }

    // Group by warehouse for efficient bulk processing
    val byWarehouse = records.groupBy { it.value().warehouseId }

    for ((warehouseId, warehouseRecords) in byWarehouse) {
      for (record in warehouseRecords) {
        try {
          applyInventoryChange(record.value())
        } catch (e: Exception) {
          logger.warn(e) { "Failed to process inventory update for SKU ${record.value().sku}, sending to DLT" }
          failureHandler.sendToDlt(record, e)
        }
      }
      logger.info { "Applied ${warehouseRecords.size} inventory changes for warehouse $warehouseId" }
    }
  }

  private fun applyInventoryChange(event: InventoryEvent) {
    require(event.sku.isNotBlank()) { "SKU must not be blank" }
    // In a real app: bulk update inventory in the database
    logger.debug { "Inventory ${event.reason}: SKU=${event.sku}, qty=${event.quantityChange}, warehouse=${event.warehouseId}" }
  }
}
