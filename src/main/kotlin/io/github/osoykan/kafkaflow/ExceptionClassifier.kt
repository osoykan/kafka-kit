package io.github.osoykan.kafkaflow

/**
 * Classification of exceptions for retry decisions.
 */
enum class ExceptionCategory {
  /** Exception is transient - retry may succeed */
  Retryable,

  /** Exception is permanent - skip retries, send to DLT immediately */
  NonRetryable
}

/**
 * Classifies exceptions to determine retry behavior.
 */
fun interface ExceptionClassifier {
  fun classify(exception: Throwable): ExceptionCategory
}

/**
 * Default classifier with configurable exception types.
 *
 * Non-retryable exceptions are typically validation errors, parsing errors,
 * or other issues that won't be fixed by retrying.
 */
class DefaultExceptionClassifier(
  private val nonRetryable: Set<Class<out Throwable>> = defaultNonRetryable,
  private val additionalNonRetryable: Set<Class<out Throwable>> = emptySet()
) : ExceptionClassifier {
  private val allNonRetryable = nonRetryable + additionalNonRetryable

  override fun classify(exception: Throwable): ExceptionCategory = if (allNonRetryable.any {
      it.isAssignableFrom(
        exception::class.java
      )
    }
  ) {
    ExceptionCategory.NonRetryable
  } else {
    ExceptionCategory.Retryable
  }

  companion object {
    /** Common non-retryable exceptions - validation and parsing errors */
    val defaultNonRetryable: Set<Class<out Throwable>> = setOf(
      IllegalArgumentException::class.java,
      IllegalStateException::class.java,
      NullPointerException::class.java,
      ClassCastException::class.java,
      NumberFormatException::class.java,
      UnsupportedOperationException::class.java,
      IndexOutOfBoundsException::class.java,
      NoSuchElementException::class.java
    )
  }
}

/**
 * Classifier that retries everything.
 * Useful for consumers that call external APIs where any error might be transient.
 */
object AlwaysRetryClassifier : ExceptionClassifier {
  override fun classify(exception: Throwable) = ExceptionCategory.Retryable
}

/**
 * Classifier that never retries.
 * Useful for idempotency-critical consumers where retrying could cause issues.
 */
object NeverRetryClassifier : ExceptionClassifier {
  override fun classify(exception: Throwable) = ExceptionCategory.NonRetryable
}

/**
 * Predefined classifier strategies for use in annotations.
 */
enum class ClassifierType {
  /** Use DefaultExceptionClassifier (validation errors → non-retryable) */
  DEFAULT,

  /** Retry all exceptions */
  ALWAYS_RETRY,

  /** Never retry any exception */
  NEVER_RETRY;

  /**
   * Creates the corresponding ExceptionClassifier instance.
   */
  fun toClassifier(additionalNonRetryable: Set<Class<out Throwable>> = emptySet()): ExceptionClassifier = when (this) {
    DEFAULT -> DefaultExceptionClassifier(additionalNonRetryable = additionalNonRetryable)
    ALWAYS_RETRY -> AlwaysRetryClassifier
    NEVER_RETRY -> NeverRetryClassifier
  }
}
