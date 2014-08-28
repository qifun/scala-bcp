/*
 * scala-bcp
 * Copyright 2014 深圳岂凡网络有限公司 (Shenzhen QiFun Network Corp., LTD)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qifun.qforce.bcp

import java.io.IOException

sealed abstract class BcpException(message: String = null, cause: Throwable = null)
  extends IOException(message, cause)

object BcpException {

  class UnknownHeadByte(message: String = null, cause: Throwable = null)
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
  
  class AlreadyReceivedFinish(message: String = "Already received the Finish Packet", cause: Throwable = null)
    extends BcpException(message, cause)

}