package com.qifun.bcp
import java.nio.ByteBuffer
import scala.annotation.tailrec
object BcpXor{
  private var key = 178
  
  def noPasswd(): Unit = {
    key = 0
  }
  def setPasswd(keys: Int): Unit = {
    key = keys
  }
  def getPasswd(): Int = {
    key
  }
  def addPasswd(byt: ByteBuffer): ByteBuffer = {
    val bytd = new Array[Byte](byt.remaining())
 
    @tailrec
    def change(i: Int): Unit = {
      if(i<byt.limit()){
        bytd(i-byt.position())=(byt.get(i)^key).toByte
        change(i+1)
      }
    }
    change(byt.position())
    ByteBuffer.wrap(bytd)
  }
  def subPasswd(byt: ByteBuffer): ByteBuffer = {
    val bytd = new Array[Byte](byt.remaining())
    
    @tailrec
    def change(i: Int): Unit = {
      if(i<byt.limit()){
        bytd(i-byt.position())=(byt.get(i)^key).toByte
        change(i+1)
      }
    }
    change(byt.position())
    ByteBuffer.wrap(bytd)
  }
  def addPasswd(byt: ByteBuffer*): Seq[ByteBuffer] = {
    val bytd = new Array[ByteBuffer](byt.length)
    
    @tailrec
    def add(i: Int): Unit = {
      if(i<byt.length){
        bytd(i) = addPasswd(byt(i))
        add(i+1)
      }
    }
    add(0)
    bytd
  }
  def subPasswd(byt: ByteBuffer*): Seq[ByteBuffer] = {
    val bytd = new Array[ByteBuffer](byt.length)
    
    @tailrec
    def add(i: Int): Unit = {
      if(i<byt.length){
        bytd(i) = subPasswd(byt(i))
        add(i+1)
      }
    }
    add(0)
    bytd
  }
}
