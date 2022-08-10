package com.datatunnel.sink.common.exception

class SinkException(message: String) extends Exception(message) {
  private var errCode = 0

  def this(errCode: Int, message: String) {
    this(message)
    this.errCode = errCode
  }

  override def getMessage: String = {
    if(errCode == 0) super.getMessage else s"(errCode: $errCode) ${super.getMessage}"
  }
}
