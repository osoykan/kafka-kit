package io.github.osoykan.kafkaflow

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class ExceptionClassifierTests :
  FunSpec({

    test("DefaultExceptionClassifier should classify IllegalArgumentException as NonRetryable") {
      val classifier = DefaultExceptionClassifier()

      val result = classifier.classify(IllegalArgumentException("bad argument"))

      result shouldBe ExceptionCategory.NonRetryable
    }

    test("DefaultExceptionClassifier should classify NullPointerException as NonRetryable") {
      val classifier = DefaultExceptionClassifier()

      val result = classifier.classify(NullPointerException("null value"))

      result shouldBe ExceptionCategory.NonRetryable
    }

    test("DefaultExceptionClassifier should classify RuntimeException as Retryable") {
      val classifier = DefaultExceptionClassifier()

      val result = classifier.classify(RuntimeException("temporary error"))

      result shouldBe ExceptionCategory.Retryable
    }

    test("DefaultExceptionClassifier should classify IOException as Retryable") {
      val classifier = DefaultExceptionClassifier()

      val result = classifier.classify(java.io.IOException("network error"))

      result shouldBe ExceptionCategory.Retryable
    }

    test("DefaultExceptionClassifier with additional non-retryable should classify custom exception") {
      class CustomValidationException : Exception("validation failed")

      val classifier = DefaultExceptionClassifier(
        additionalNonRetryable = setOf(CustomValidationException::class.java)
      )

      val result = classifier.classify(CustomValidationException())

      result shouldBe ExceptionCategory.NonRetryable
    }

    test("AlwaysRetryClassifier should classify all exceptions as Retryable") {
      AlwaysRetryClassifier.classify(IllegalArgumentException("bad")) shouldBe ExceptionCategory.Retryable
      AlwaysRetryClassifier.classify(NullPointerException()) shouldBe ExceptionCategory.Retryable
      AlwaysRetryClassifier.classify(RuntimeException("any error")) shouldBe ExceptionCategory.Retryable
    }

    test("NeverRetryClassifier should classify all exceptions as NonRetryable") {
      NeverRetryClassifier.classify(RuntimeException("temporary")) shouldBe ExceptionCategory.NonRetryable
      NeverRetryClassifier.classify(java.io.IOException("network")) shouldBe ExceptionCategory.NonRetryable
      NeverRetryClassifier.classify(Exception("any")) shouldBe ExceptionCategory.NonRetryable
    }

    test("ClassifierType.DEFAULT should create DefaultExceptionClassifier") {
      val classifier = ClassifierType.DEFAULT.toClassifier()

      classifier.classify(IllegalArgumentException()) shouldBe ExceptionCategory.NonRetryable
      classifier.classify(RuntimeException()) shouldBe ExceptionCategory.Retryable
    }

    test("ClassifierType.ALWAYS_RETRY should create AlwaysRetryClassifier") {
      val classifier = ClassifierType.ALWAYS_RETRY.toClassifier()

      classifier.classify(IllegalArgumentException()) shouldBe ExceptionCategory.Retryable
    }

    test("ClassifierType.NEVER_RETRY should create NeverRetryClassifier") {
      val classifier = ClassifierType.NEVER_RETRY.toClassifier()

      classifier.classify(RuntimeException()) shouldBe ExceptionCategory.NonRetryable
    }

    test("ClassifierType.DEFAULT with additional should include custom non-retryable") {
      class MyException : Exception()

      val classifier = ClassifierType.DEFAULT.toClassifier(
        additionalNonRetryable = setOf(MyException::class.java)
      )

      classifier.classify(MyException()) shouldBe ExceptionCategory.NonRetryable
      classifier.classify(RuntimeException()) shouldBe ExceptionCategory.Retryable
    }

    test("DefaultExceptionClassifier should handle subclasses of non-retryable exceptions") {
      // NumberFormatException extends IllegalArgumentException
      val classifier = DefaultExceptionClassifier()

      classifier.classify(NumberFormatException("not a number")) shouldBe ExceptionCategory.NonRetryable
    }
  })
