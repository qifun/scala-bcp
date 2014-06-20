package com.qifun.qforce.bcp.server

import com.qifun.statelessFuture.io.SocketInputStream
import com.qifun.statelessFuture.Future
import java.io.EOFException
import java.nio.ByteBuffer
import java.io.IOException
import scala.annotation.tailrec
import com.qifun.statelessFuture.io.SocketWritingQueue
import scala.collection.mutable.ArrayBuffer
import com.qifun.qforce.bcp.server.Bcp._

private[server] object BcpIo {

  final def receiveUnsignedVarint(socket: SocketInputStream): Future[Int] = {
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
  final def writeUnsignedVarint(buffer: ByteBuffer, value: Int) {
    if ((value & 0xFFFFFF80) == 0) {
      buffer.put(value.toByte)
      return ;
    } else {
      buffer.put(((value & 0x7F) | 0x80).toByte)
      writeUnsignedVarint(buffer, value >>> 7)
    }
  }

  final def enqueue(stream: SocketWritingQueue, pack: Acknowledge.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](Acknowledge.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: Renew.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](Acknowledge.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: Finish.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](Finish.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: RetransmissionFinish) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(RetransmissionFinish.HeadByte)
    writeUnsignedVarint(headBuffer, pack.connectionId)
    writeUnsignedVarint(headBuffer, pack.packId)
    headBuffer.flip()
    stream.enqueue(headBuffer)
  }

  final def enqueue(stream: SocketWritingQueue, pack: RetransmissionData) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(RetransmissionData.HeadByte)
    writeUnsignedVarint(headBuffer, pack.connectionId)
    writeUnsignedVarint(headBuffer, pack.packId)
    writeUnsignedVarint(headBuffer, pack.buffer.length)
    headBuffer.flip()
    stream.enqueue((headBuffer +: pack.buffer.view): _*)
  }

  final def enqueue(stream: SocketWritingQueue, pack: Data) {
    val headBuffer = ByteBuffer.allocate(20)
    headBuffer.put(Data.HeadByte)
    writeUnsignedVarint(headBuffer, pack.buffer.length)
    headBuffer.flip()
    stream.enqueue((headBuffer +: pack.buffer.view): _*)
  }

  final def enqueue(stream: SocketWritingQueue, pack: ShutDownInput.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](ShutDownInput.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: ShutDownOutput.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](ShutDownOutput.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: HeartBeat.type) {
    stream.enqueue(ByteBuffer.wrap(Array[Byte](HeartBeat.HeadByte)))
  }

  final def enqueue(stream: SocketWritingQueue, pack: ServerToClient) {
    pack match {
      case pack @ Acknowledge => {
        enqueue(stream, pack)
      }
      case pack: Data => {
        enqueue(stream, pack)
      }
      case pack @ Finish => {
        enqueue(stream, pack)
      }
      case pack: RetransmissionData => {
        enqueue(stream, pack)
      }
      case pack: RetransmissionFinish => {
        enqueue(stream, pack)
      }
      case pack @ ShutDownOutput => {
        enqueue(stream, pack)
      }
      case pack @ ShutDownInput => {
        enqueue(stream, pack)
      }
      case pack @ Renew => {
        enqueue(stream, pack)
      }
      case pack @ HeartBeat => {
        enqueue(stream, pack)
      }
    }
  }

  @throws(classOf[IOException])
  final def receive(stream: SocketInputStream) = Future[ClientToServer] {
    stream.available_=(1).await
    stream.read() match {
      case Data.HeadByte => {
        val length = receiveUnsignedVarint(stream).await
        stream.available_=(length).await
        val buffer = new ArrayBuffer[ByteBuffer]
        stream.move(buffer, length)
        Data(buffer)
      }
      case RetransmissionData.HeadByte => {
        val connectionId = receiveUnsignedVarint(stream).await
        val packId = receiveUnsignedVarint(stream).await
        val length = receiveUnsignedVarint(stream).await
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
      case Acknowledge.HeadByte => Acknowledge
      case RenewRequest.HeadByte => {
        RenewRequest(receiveSessionId(stream).await)
      }
      case Finish.HeadByte => {
        Finish
      }
      case ShutDownInput.HeadByte => {
        ShutDownInput
      }
      case ShutDownOutput.HeadByte => {
        ShutDownOutput
      }
      case HeartBeat.HeadByte => {
        HeartBeat
      }
      case _ => throw new BcpException.UnknownHeadByte
    }

  }

  final def receiveHead(stream: SocketInputStream) = Future {
    val sessionId = receiveSessionId(stream).await
    val connectionId = receiveUnsignedVarint(stream).await
    ConnectionHead(sessionId, connectionId)
  }

  private def receiveSessionId(stream: SocketInputStream) = Future[Array[Byte]] {
    stream.available_=(NumBytesSessionId).await
    var sessionId = Array.ofDim[Byte](NumBytesSessionId)
    stream.read(sessionId)
    sessionId
  }
}