package com.qifun.qforce.bcp.server
import scala.annotation.switch
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.io.SocketInputStream
import java.io.EOFException
import java.io.InputStream
import scala.concurrent.duration._

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

  case class Data(data: InputStream) extends ServerToClient with ClientToServer
  object Data {
    final val HeadByte = 0
  }

  case object EndData extends ServerToClient with ClientToServer {
    final val HeadByte = 1
  }

  case object EndDataAndWait extends ServerToClient with ClientToServer {
    final val HeadByte = 2
  }

  case object Renew extends ServerToClient {
    final val HeadByte = 3
  }

  case object RenewRequest extends ClientToServer {
    final val HeadByte = 4
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
            throw new BcpException("The varint is too big to read!")
          }

        }
      }
    }
    readRestBytes(0, 0)
  }

  final def readBcp(stream: SocketInputStream) = Future[ClientToServer] {
    stream.available_=(1).await
    (stream.read(): @switch) match {
      case -1 => throw new EOFException
      case Data.HeadByte => {
        stream.available_=(readVarint(stream).await).await
        val data = Data(stream.duplicate)
        stream.skip(stream.available)
        data
      }
      case EndDataAndWait.HeadByte => EndDataAndWait
      case EndData.HeadByte => EndData
      case RenewRequest.HeadByte => RenewRequest
    }

  }

  final def readSessionId(stream: SocketInputStream) = Future[Array[Byte]] {
    stream.available_=(16).await
    var sessionId = Array.ofDim[Byte](16)
    stream.read(sessionId)
    sessionId
  }

}