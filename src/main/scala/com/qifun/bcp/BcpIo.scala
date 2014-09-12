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

package com.qifun.bcp

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import scala.annotation.tailrec
import com.qifun.bcp.Bcp._
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.util.io.SocketInputStream
import com.qifun.statelessFuture.util.io.SocketWritingQueue
import java.io.EOFException
import java.io.IOException
import scala.collection.mutable.ArrayBuffer

private[bcp] object BcpIo {

  private final def receiveUnsignedVarint(stream: SocketInputStream): Future[Int] = {
    def receiveRestBytes(result: Int, i: Int): Future[Int] = Future[Int] {
      (stream.available_=(1)).await
      stream.read() match {
        case -1 => throw new EOFException
        case b => {
          if (i < 32) {
            if (b >= 0x80) {
              receiveRestBytes(
                result | ((b & 0x7f) << i),
                i + 7).await
            } else {
              result | (b << i)
            }
          } else {
            throw new BcpException.VarintTooBig
          }

        }
      }
    }
    receiveRestBytes(0, 0)
  }

  @tailrec
  private final def writeUnsignedVarint(buffer: ByteBuffer, value: Int) {
    if ((value & 0xFFFFFF80) == 0) {
      buffer.put(value.toByte)
      return ;
    } else {
      buffer.put(((value & 0x7F) | 0x80).toByte)
      writeUnsignedVarint(buffer, value >>> 7)
    }
  }

  final def enqueue(queue: SocketWritingQueue, pack: Acknowledge.type) {
    queue.enqueue(ByteBuffer.wrap(Array[Byte](Acknowledge.HeadByte)))
  }

  final def enqueue(queue: SocketWritingQueue, pack: Finish.type) {
    queue.enqueue(ByteBuffer.wrap(Array[Byte](Finish.HeadByte)))
  }

  final def enqueue(queue: SocketWritingQueue, pack: RetransmissionFinish) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(RetransmissionFinish.HeadByte)
    writeUnsignedVarint(headBuffer, pack.connectionId)
    writeUnsignedVarint(headBuffer, pack.packId)
    headBuffer.flip()
    queue.enqueue(headBuffer)
  }

  final def enqueue(queue: SocketWritingQueue, pack: RetransmissionData) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(RetransmissionData.HeadByte)
    writeUnsignedVarint(headBuffer, pack.connectionId)
    writeUnsignedVarint(headBuffer, pack.packId)
    writeUnsignedVarint(headBuffer, pack.buffers.view.map(_.remaining).sum)
    headBuffer.flip()
    val newBuffer = for (buffer <- pack.buffers) yield buffer.duplicate()
    queue.enqueue((headBuffer +: newBuffer.view): _*)
  }

  final def enqueue(queue: SocketWritingQueue, pack: Data) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(Data.HeadByte)
    writeUnsignedVarint(headBuffer, pack.buffers.view.map(_.remaining).sum)
    headBuffer.flip()
    val newBuffer = for (buffer <- pack.buffers) yield buffer.duplicate()
    queue.enqueue((headBuffer +: newBuffer.view): _*)
  }

  final def enqueue(queue: SocketWritingQueue, pack: ShutDown.type) {
    queue.enqueue(ByteBuffer.wrap(Array[Byte](ShutDown.HeadByte)))
  }

  final def enqueue(queue: SocketWritingQueue, pack: HeartBeat.type) {
    queue.enqueue(ByteBuffer.wrap(Array[Byte](HeartBeat.HeadByte)))
  }

  final def enqueue(queue: SocketWritingQueue, pack: Packet) {
    pack match {
      case pack @ Acknowledge => {
        enqueue(queue, pack)
      }
      case pack: Data => {
        enqueue(queue, pack)
      }
      case pack @ Finish => {
        enqueue(queue, pack)
      }
      case pack: RetransmissionData => {
        enqueue(queue, pack)
      }
      case pack: RetransmissionFinish => {
        enqueue(queue, pack)
      }
      case pack @ ShutDown => {
        enqueue(queue, pack)
      }
      case pack @ HeartBeat => {
        enqueue(queue, pack)
      }
    }
  }

  @throws(classOf[IOException])
  final def receive(stream: SocketInputStream) = Future[ClientToServer] {
    stream.available_=(1).await
    stream.read() match {
      case Data.HeadByte => {
        val length = receiveUnsignedVarint(stream).await
        if (length > MaxDataSize) {
          throw new BcpException.DataTooBig
        }
        stream.available_=(length).await
        val buffer = new ArrayBuffer[ByteBuffer]
        stream.move(buffer, length)
        Data(buffer)
      }
      case RetransmissionData.HeadByte => {
        val connectionId = receiveUnsignedVarint(stream).await
        val packId = receiveUnsignedVarint(stream).await
        val length = receiveUnsignedVarint(stream).await
        if (length > MaxDataSize) {
          throw new BcpException.DataTooBig
        }
        stream.available_=(length).await
        val buffer = new ArrayBuffer[ByteBuffer]
        stream.move(buffer, length)
        RetransmissionData(connectionId, packId, buffer)
      }
      case RetransmissionFinish.HeadByte => {
        val connectionId = receiveUnsignedVarint(stream).await
        val packId = receiveUnsignedVarint(stream).await
        RetransmissionFinish(connectionId, packId)
      }
      case Acknowledge.HeadByte => {
        Acknowledge
      }
      case Finish.HeadByte => {
        Finish
      }
      case ShutDown.HeadByte => {
        ShutDown
      }
      case HeartBeat.HeadByte => {
        HeartBeat
      }
      case _ => throw new BcpException.UnknownHeadByte
    }

  }

  private def boolToInt(bool: Boolean) = if (bool) 1 else 0
  private def intToBool(int: Int) = if (int == 0) false else true

  final def receiveHead(stream: SocketInputStream) = Future {
    val sessionId = receiveSessionId(stream).await
    val isRenew = receiveUnsignedVarint(stream).await
    val connectionId = receiveUnsignedVarint(stream).await
    ConnectionHead(sessionId, intToBool(isRenew), connectionId)
  }

  private def receiveSessionId(stream: SocketInputStream) = Future[Array[Byte]] {
    stream.available_=(NumBytesSessionId).await
    var sessionId = Array.ofDim[Byte](NumBytesSessionId)
    stream.read(sessionId)
    sessionId
  }

  final def enqueueHead(stream: SocketWritingQueue, head: ConnectionHead) = {
    val ConnectionHead(sessionId, isRenew, connectionId) = head
    val headBuffer = ByteBuffer.allocate(NumBytesSessionId + 1 + 5)
    headBuffer.put(sessionId)
    writeUnsignedVarint(headBuffer, boolToInt(isRenew))
    writeUnsignedVarint(headBuffer, connectionId)
    headBuffer.flip()
    stream.enqueue(headBuffer)
  }
}