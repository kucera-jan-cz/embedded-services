package com.github.embedded.utils.logging

import org.slf4j.Logger

trait Slf4jLogging {

  implicit class Implicits(logger: Logger) {
    def trace(format: String, args: Any*): Unit = {
      logger.trace(format, args.map(_.asInstanceOf[AnyRef]): _*)
    }

    def debug(format: String, args: Any*): Unit = {
      logger.debug(format, args.map(_.asInstanceOf[AnyRef]): _*)
    }

    def info(format: String, args: Any*): Unit = {
      logger.info(format, args.map(_.asInstanceOf[AnyRef]): _*)
    }

    def warn(format: String, args: Any*): Unit = {
      logger.warn(format, args.map(_.asInstanceOf[AnyRef]): _*)
    }

    def error(format: String, args: Any*): Unit = {
      logger.error(format, args.map(_.asInstanceOf[AnyRef]): _*)
    }
  }
}
