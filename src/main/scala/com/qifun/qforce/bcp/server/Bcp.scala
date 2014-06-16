package com.qifun.qforce.bcp.server
import scala.annotation.switch
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.io.SocketInputStream
import java.io.EOFException
import java.io.InputStream
import scala.concurrent.duration._
import com.qifun.statelessFuture.io.SocketWritingQueue
import java.nio.ByteBuffer
import scala.collection.generic.Growable
import org.apache.commons.io.IOUtils
import scala.annotation.tailrec
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.io.ObjectInputStream
import java.io.DataInputStream

object Bcp {

  /**
   * 服务器多长时间收不到心跳包就杀掉TCP连接。
   */
  final def ServerReadingTimeout = 5.seconds

  /**
   * 服务器多长时间发不出数据就杀掉TCP连接。
   *
   * 发数据超时只可能因为TCP缓冲区满，
   * 只有发送超大数据包时，客户端很慢，才会发生这种情况，可能性微乎其微。
   */
  final def ServerWritingTimeout = 1.seconds

  sealed trait ServerToClient

  sealed trait ClientToServer

  /**
   * @returns Number of bytes read
   */
  @tailrec
  private def readByteBuffers(output: Growable[ByteBuffer], inputStream: InputStream, count: Int = 0): Int = {
    // 多读一个字节，以便检测是否会EOF
    val array = Array.ofDim[Byte](inputStream.available + 1)
    val numBytesRead = IOUtils.read(inputStream, array)
    if (numBytesRead == 0) {
      count
      // EOF
    } else if (numBytesRead == array.length) {
      output += ByteBuffer.wrap(array)
      readByteBuffers(output, inputStream, numBytesRead + count)
    } else {
      ByteBuffer.wrap(array, 0, numBytesRead)
      numBytesRead + count
    }
  }

  final case class Data(packId: Int, inputStream: InputStream) extends ServerToClient with ClientToServer
  object Data {
    final val HeadByte: Byte = 0
  }

  case object EndData extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 1
  }

  case object EndDataAndWait extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 2
  }

  case object Renew extends ServerToClient {
    final val HeadByte: Byte = 3
  }

  case object RenewRequest extends ClientToServer {
    final val HeadByte: Byte = 4
  }

  private def readVarint(socket: SocketInputStream): Future[Int] = {
    def readRestBytes(result: Int, i: Int): Future[Int] = Future[Int] {
      (socket.available_=(1)).await
      socket.read() match {
        case -1 => throw new EOFException
        case b => {
          if (i < 32) {
            if (b >= 0x80) {
              readRestBytes(
                result | ((b & 0x7f) << i),
                i + 7).await
            } else {
              result | (b << i)
            }
          } else {
            throw BcpException.VarintTooBig
          }

        }
      }
    }
    readRestBytes(0, 0)
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

  final def enqueue(stream: SocketWritingQueue, pack: ServerToClient) {
    pack match {
      case Renew => stream.enqueue(ByteBuffer.wrap(Array[Byte](Renew.HeadByte)))
      case EndDataAndWait => stream.enqueue(ByteBuffer.wrap(Array[Byte](EndDataAndWait.HeadByte)))
      case EndData => stream.enqueue(ByteBuffer.wrap(Array[Byte](EndData.HeadByte)))
      case Data(packId, data) => {
        val headBuffer = ByteBuffer.allocate(10)
        headBuffer.put(Data.HeadByte)
        headBuffer.putInt(packId)
        val bufferBuilder = ArrayBuffer[ByteBuffer](headBuffer)
        val length = readByteBuffers(bufferBuilder, data)
        writeVarint(headBuffer, length)
        headBuffer.flip()
        stream.enqueue(bufferBuilder: _*)
      }
    }
  }

  private def readBigEndianInt(inputStream: InputStream) = {
    val byte1 = inputStream.read()
    val byte2 = inputStream.read()
    val byte3 = inputStream.read()
    val byte4 = inputStream.read()
    if ((byte1 | byte2 | byte3 | byte4) < 0) {
      throw BcpException.Eof
    }
    ((byte1 << 24) + (byte2 << 16) + (byte3 << 8) + (byte4 << 0))
  }

  final def receive(stream: SocketInputStream) = Future[ClientToServer] {
    stream.available_=(1).await
    stream.read() match {
      case Data.HeadByte => {
        val packId = readBigEndianInt(stream)
        val length = readVarint(stream).await
        stream.available_=(length).await
        val data = Data(packId, stream.duplicate)
        stream.skip(stream.available)
        data
      }
      case EndDataAndWait.HeadByte => EndDataAndWait
      case EndData.HeadByte => EndData
      case RenewRequest.HeadByte => RenewRequest
      case -1 => throw BcpException.Eof
      case _ => throw BcpException.UnknownHeadByte
    }

  }

  final def readSessionId(stream: SocketInputStream) = Future[Array[Byte]] {
    stream.available_=(16).await
    var sessionId = Array.ofDim[Byte](16)
    stream.read(sessionId)
    sessionId
  }

}