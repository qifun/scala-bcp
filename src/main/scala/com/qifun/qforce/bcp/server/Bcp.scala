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
import java.io.IOException

/**
 * BCP协议相关的数据结构和常量
 */
object Bcp {

  /**
   * 最多缓存多少个离线包
   */
  final val MaxOfflinePack = 200

  final val MaxConnectionsPerSession = 3

  /**
   * 当一个拉取数据的TCP连接空闲多长时间以后，服务端发一个心跳包
   */
  final val ServerHeartBeatDelay: FiniteDuration = 3.seconds

  /**
   * 服务器多长时间收不到心跳包就杀掉TCP连接。
   */
  final val ServerReadingTimeout = 6.seconds

  /**
   * 服务器多长时间发不出数据就杀掉TCP连接。
   *
   * 发数据超时只可能因为TCP缓冲区满，
   * 只有发送超大数据包时，客户端很慢，才会发生这种情况，可能性微乎其微。
   */
  final val ServerWritingTimeout = 1.seconds

  /**
   * Session ID由多少字节构成
   */
  final val NumBytesSessionId = 16

  sealed trait ServerToClient

  sealed trait ClientToServer

  /**
   * 需要回复[[Acknowledge]]的协议
   */
  sealed trait AcknowledgeRequired

  /**
   * 重传的数据
   */
  sealed trait Retransmission {
    val connectionId: Int
    val packId: Int
  }

  final case class ConnectionHead(sessionId: Array[Byte], connectionId: Int)

  final case class Data(buffer: Seq[ByteBuffer])
    extends ServerToClient with ClientToServer with AcknowledgeRequired
  object Data {
    final val HeadByte: Byte = 0
  }

  case object Acknowledge extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 1
  }

  case class RetransmissionData(connectionId: Int, packId: Int, buffer: Seq[ByteBuffer])
    extends ServerToClient with ClientToServer with AcknowledgeRequired with Retransmission
  object RetransmissionData {
    final val HeadByte: Byte = 2
  }

  case object Renew extends ServerToClient {
    final val HeadByte: Byte = 3
  }
  object RenewRequest {
    final val HeadByte: Byte = 4

  }
  final case class RenewRequest(newSessionId: Array[Byte]) extends ClientToServer

  /**
   * 结束一个TCP连接，相当于TCP FIN。
   *
   * @note 不能直接用TCP FIN是因为一方发送Finish后还可能继续发Acknowledge。
   * 而在调用shutdown发送TCP FIN后，就没办法再发送Acknowledge了。
   */
  case object Finish
    extends ServerToClient with ClientToServer with AcknowledgeRequired {
    final val HeadByte: Byte = 5
  }

  case class RetransmissionFinish(connectionId: Int, packId: Int)
    extends ServerToClient with ClientToServer with AcknowledgeRequired with Retransmission
  object RetransmissionFinish {
    final val HeadByte: Byte = 6
  }

  /**
   * 发送[[ShutDownInput]]的一端不再接收整个[[BcpServer.Session]]的任何新数据。
   */
  case object ShutDownInput
    extends ServerToClient with ClientToServer with AcknowledgeRequired {
    final val HeadByte: Byte = 7
  }

  /**
   * 发送[[ShutDownInput]]的一端不再向[[BcpServer.Session]]发送任何新数据。
   */
  case object ShutDownOutput
    extends ServerToClient with ClientToServer with AcknowledgeRequired {
    final val HeadByte: Byte = 8
  }

  case object HeartBeat
    extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 9
  }

}