package com.data.integration.core.exception

class ParseException(message: String, cause: Throwable) extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)

}
