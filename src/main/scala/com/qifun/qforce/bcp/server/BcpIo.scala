package com.qifun.qforce.bcp.server

import com.qifun.statelessFuture.io.SocketInputStream
import com.qifun.statelessFuture.Future
import java.io.EOFException
import java.nio.ByteBuffer
import java.io.IOException
import scala.annotation.tailrec
import com.qifun.statelessFuture.io.SocketWritingQueue
import scala.collection.mutable.ArrayBuffer

private[server] object BcpIo {

  import Bcp._

  private def receiveVarint(socket: SocketInputStream): Future[Int] = {
    def receiveRestBytes(result: Int, i: Int): Future[Int] = Future[Int] {
      (socket.available_=(1)).await
      socket.read() match {
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
  private def writeVarint(buffer: ByteBuffer, value: Int) {
    if ((value & 0xFFFFFF80) == 0) {
      buffer.put(value.toByte)
      return ;
    } else {
      buffer.put(((value & 0x7F) | 0x80).toByte)
      writeVarint(buffer, value >>> 7)
    }
  }

  final def enqueueAcknowledge(stream: SocketWritingQueue) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](Acknowledge.HeadByte)))
  }

  //  final def enqueueEndData(stream: SocketWritingQueue) {
  //    stream.enqueue(ByteBuffer.wrap(Array[Byte](EndData.HeadByte)))
  //  }
  //
  //  final def enqueueEndDataAndWait(stream: SocketWritingQueue) {
  //    stream.enqueue(ByteBuffer.wrap(Array[Byte](EndDataAndWait.HeadByte)))
  //  }

  //  final def enqueue(stream: SocketWritingQueue, pack: ServerToClient) {
  //    pack match {
  //      case Renew => stream.enqueue(ByteBuffer.wrap(Array[Byte](Renew.HeadByte)))
  //      case EndDataAndWait => stream.enqueue(ByteBuffer.wrap(Array[Byte](EndDataAndWait.HeadByte)))
  //      case EndData => stream.enqueue(ByteBuffer.wrap(Array[Byte](EndData.HeadByte)))
  //      case Data(packId, data) => {
  //        val headBuffer = ByteBuffer.allocate(10)
  //        headBuffer.put(Data.HeadByte)
  //        headBuffer.putInt(packId)
  //        val bufferBuilder = ArrayBuffer[ByteBuffer](headBuffer)
  //        val length = readByteBuffers(bufferBuilder, data)
  //        writeVarint(headBuffer, length)
  //        headBuffer.flip()
  //        stream.enqueue(bufferBuilder: _*)
  //      }
  //    }
  //  }

  private def receiveBigEndianInt(inputStream: SocketInputStream) = Future {
    inputStream.available_=(4).await
    val byte1 = inputStream.read()
    val byte2 = inputStream.read()
    val byte3 = inputStream.read()
    val byte4 = inputStream.read()
    ((byte1 << 24) + (byte2 << 16) + (byte3 << 8) + (byte4 << 0))
  }

  @throws(classOf[IOException])
  final def receive(stream: SocketInputStream) = Future[ClientToServer] {
    stream.available_=(1).await
    stream.read() match {
      case Data.HeadByte => {
        val length = receiveVarint(stream).await
        stream.available_=(length).await
        val buffer = new ArrayBuffer[ByteBuffer]
        stream.move(buffer, length)
        Data(buffer)
      }
      case RetransmissionData.HeadByte => {
        val connectionId = receiveVarint(stream).await
        val packId = receiveVarint(stream).await
        val length = receiveVarint(stream).await
        stream.available_=(length).await
        val buffer = new ArrayBuffer[ByteBuffer]
        stream.move(buffer, length)
        RetransmissionData(connectionId, packId, buffer)
      }
      case Acknowledge.HeadByte => Acknowledge
      case RenewRequest.HeadByte => {
        RenewRequest(receiveSessionId(stream).await)
      }
      case _ => throw new BcpException.UnknownHeadByte
    }

  }

  final def receiveHead(stream: SocketInputStream) = Future {
    val sessionId = receiveSessionId(stream).await
    val connectionId = receiveVarint(stream).await
    ConnectionHead(sessionId, connectionId)
  }

  private def receiveSessionId(stream: SocketInputStream) = Future[Array[Byte]] {
    stream.available_=(NumBytesSessionId).await
    var sessionId = Array.ofDim[Byte](NumBytesSessionId)
    stream.read(sessionId)
    sessionId
  }
}