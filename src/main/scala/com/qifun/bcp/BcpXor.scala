package com.qifun.bcp
import java.nio.ByteBuffer
import scala.annotation.tailrec

trait BcpXor extends BcpCrypto {
  val key: Byte = 100

  abstract override def dataDecrypt(buffer: ByteBuffer*): Seq[ByteBuffer] = {
    decrypt(buffer: _*)
  }

  abstract override def dataEncrypt(buffer: ByteBuffer*): Seq[ByteBuffer] = {
    encrypt(buffer: _*)
  }

  private def encrypt(byte: ByteBuffer): ByteBuffer = {
    val byteCrypto = new Array[Byte](byte.remaining())

    @tailrec
    def change(i: Int): Unit = {
      if (i < byte.limit()) {
        byteCrypto(i - byte.position()) = (byte.get(i) ^ key).toByte
        change(i + 1)
      }
    }
    change(byte.position())
    ByteBuffer.wrap(byteCrypto)
  }

  private def decrypt(byte: ByteBuffer): ByteBuffer = {
    val byteCrypto = new Array[Byte](byte.remaining())

    @tailrec
    def change(i: Int): Unit = {
      if (i < byte.limit()) {
        byteCrypto(i - byte.position()) = (byte.get(i) ^ key).toByte
        change(i + 1)
      }
    }
    change(byte.position())
    ByteBuffer.wrap(byteCrypto)
  }

  private def encrypt(byte: ByteBuffer*): Seq[ByteBuffer] = {
    val byteCrypto = new Array[ByteBuffer](byte.length)

    @tailrec
    def add(i: Int): Unit = {
      if (i < byte.length) {
        byteCrypto(i) = encrypt(byte(i))
        add(i + 1)
      }
    }
    add(0)
    byteCrypto
  }

  private def decrypt(byte: ByteBuffer*): Seq[ByteBuffer] = {
    val byteCrypto = new Array[ByteBuffer](byte.length)

    @tailrec
    def add(i: Int): Unit = {
      if (i < byte.length) {
        byteCrypto(i) = encrypt(byte(i))
        add(i + 1)
      }
    }
    add(0)
    byteCrypto
  }
}
