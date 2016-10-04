package com.github.embedded.utils.logging

import ch.qos.logback.classic.spi.{ILoggingEvent, LoggingEvent}
import ch.qos.logback.core.AppenderBase
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory

class Slf4jLoggingTest extends FunSuite with Slf4jLogging with BeforeAndAfterAll with TableDrivenPropertyChecks {
	private val logger = LoggerFactory.getLogger(classOf[Slf4jLoggingTest])
	val appender = new TestAppender

	override def beforeAll(): Unit = {
		logger.asInstanceOf[ch.qos.logback.classic.Logger].addAppender(appender)
		appender.start()
	}

	val examples =
		Table(
			("{}", "XX", "XX")
		)

	test("SLF4J formatting") {
		val (iVal, fVal) = (1, 1.0)
		val (aVal, bVal, x) = ("A", "B", "XX")
		logger.info("{}", x)
		assert(appender.message == "XX")

		logger.info("|{}|{}|", Array(aVal, bVal): _*)
		assert(appender.message == "|A|B|")

		logger.info("|{}|{}|", iVal, fVal)
		assert(appender.message == "|1|1.0|")

		logger.info("|{}|{}|{}|", aVal, bVal, x)
		assert(appender.message == "|A|B|XX|")

		logger.info("|{}|{}|{}|", iVal, fVal, Array(aVal, bVal))
		assert(appender.message == "|1|1.0|[A, B]|")

		logger.info("|{}|{}|{}|{}|", iVal, Array(aVal, bVal), x, aVal)
		assert(appender.message == "|1|[A, B]|XX|A|")
	}
}

class TestAppender extends AppenderBase[ILoggingEvent] {
	var message: String = null

	override def append(eventObject: ILoggingEvent): Unit = {
		message = eventObject.getFormattedMessage
	}
}
