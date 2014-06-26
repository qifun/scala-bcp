package com.qifun.qforce.bcp

import java.io.IOException

sealed abstract class BcpException(message: String = null, cause: Throwable = null)
  extends IOException(message, cause)

object BcpException {

  class UnknownHeadByte(message: String = null, cause: Throwable = null)
    extends BcpException(message, cause)

  class SendingQueueIsFull(message: String = null, cause: Throwable = null)
    extends BcpException(message, cause)

  class VarintTooBig(message: String = "The varint is too big to read!", cause: Throwable = null)
    extends BcpException(message, cause)

  /**
   * 数据包大小超过[[Bcp.MaxDataSize]]。
   * 
   * 说明客户端乱发数据，应该断掉连接。
   */
  class DataTooBig(message: String = "The data received was too big!", cause: Throwable = null)
    extends BcpException(message, cause)

}