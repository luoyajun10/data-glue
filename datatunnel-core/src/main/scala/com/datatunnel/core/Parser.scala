package com.datatunnel.core

trait Parser[T <: AnyRef] {

  /**
    * protobuf字节码解析
    * @param bytes protobuf字节码
    * @return
    */
  def parse(bytes: Array[Byte]) : T

}
