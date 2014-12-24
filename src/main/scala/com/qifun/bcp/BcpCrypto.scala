package com.qifun.bcp

import java.nio.ByteBuffer

trait BcpCrypto {

  def dataDecrypt(buffer: ByteBuffer*): Seq[ByteBuffer]

  def dataEncrypt(buffer: ByteBuffer*): Seq[ByteBuffer]

}