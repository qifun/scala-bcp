package com.qifun.bcp

import java.nio.ByteBuffer
import BcpXor._

trait DataOperation{
  
  def dataDecryptOperation(buffer: ByteBuffer*): Seq[ByteBuffer] 
  
  def dataEncryptOperation(buffer: ByteBuffer*): Seq[ByteBuffer] 
  
}
trait Crypto extends DataOperation{
  
  abstract override def dataDecryptOperation(buffer: ByteBuffer*): Seq[ByteBuffer] = {
    decrypt(buffer: _*)
  }
  
  abstract override def dataEncryptOperation(buffer: ByteBuffer*): Seq[ByteBuffer] = {
    encrypt(buffer: _*)
  }
}