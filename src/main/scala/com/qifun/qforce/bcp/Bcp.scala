package com.qifun.qforce.bcp
import scala.concurrent.duration._
import java.nio.ByteBuffer

/**
 * BCP协议相关的数据结构和常量
 */
private[bcp] object Bcp {

  /**
   * 最多缓存多少个离线包
   *
   * @group Constants
   */
  final val MaxOfflinePack = 200

  /**
   * 每个会话最多允许多少个TCP连接
   *
   * @group Constants
   */
  final val MaxConnectionsPerSession = 3

  /**
   * 当一个拉取数据的TCP连接空闲多长时间以后，服务端发一个心跳包
   *
   * @group Constants
   */
  final val ServerHeartBeatDelay: FiniteDuration = 3.seconds

  /**
   * 服务器多长时间收不到心跳包就杀掉TCP连接。
   *
   * @group Constants
   */
  final val ServerReadingTimeout = 6.seconds

  /**
   * 服务器多长时间发不出数据就杀掉TCP连接。
   *
   * 发数据超时只可能因为TCP缓冲区满，
   * 只有发送超大数据包时，客户端很慢，才会发生这种情况，可能性微乎其微。
   *
   * @group Constants
   */
  final val ServerWritingTimeout = 1.seconds

  /**
   * Session ID由多少字节构成
   *
   * @group Constants
   */
  final val NumBytesSessionId = 16

  /**
   * @group Protocols
   */
  final case class ConnectionHead(sessionId: Array[Byte], connectionId: Int)

  /**
   * @group Protocols
   */
  sealed trait Packet

  /**
   * @group Protocols
   */
  sealed trait ServerToClient extends Packet

  /**
   * @group Protocols
   */
  sealed trait ClientToServer extends Packet

  /**
   * 需要回复[[Acknowledge]]的协议
   *
   * @group Protocols
   */
  sealed trait AcknowledgeRequired extends Packet

  /**
   * 重传的数据
   *
   * @group Protocols
   */
  sealed trait Retransmission extends Packet {
    val connectionId: Int
    val packId: Int
  }

  /**
   * @group Protocols
   */
  final case class Data(buffer: Seq[ByteBuffer])
    extends ServerToClient with ClientToServer with AcknowledgeRequired

  /**
   * @group Protocols
   */
  object Data {
    final val HeadByte: Byte = 0
  }

  /**
   * @group Protocols
   */
  case object Acknowledge extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 1
  }

  /**
   * @group Protocols
   */
  case class RetransmissionData(connectionId: Int, packId: Int, buffer: Seq[ByteBuffer])
    extends ServerToClient with ClientToServer with AcknowledgeRequired with Retransmission

  /**
   * @group Protocols
   */
  object RetransmissionData {
    final val HeadByte: Byte = 2
  }

  /**
   * @group Protocols
   */
  case object Renew extends ClientToServer {
    final val HeadByte: Byte = 3
  }

  /**
   * 结束一个TCP连接，相当于TCP FIN。
   *
   * @note 不能直接用TCP FIN是因为一方发送Finish后还可能继续发Acknowledge。
   * 而在调用shutdown发送TCP FIN后，就没办法再发送Acknowledge了。
   *
   * @group Protocols
   */
  case object Finish
    extends ServerToClient with ClientToServer with AcknowledgeRequired {
    final val HeadByte: Byte = 5
  }

  /**
   * @group Protocols
   */
  case class RetransmissionFinish(connectionId: Int, packId: Int)
    extends ServerToClient with ClientToServer with AcknowledgeRequired with Retransmission

  /**
   * @group Protocols
   */
  object RetransmissionFinish {
    final val HeadByte: Byte = 6
  }

  /**
   * @group Protocols
   */
  case object ShutDown
    extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 7
  }

  /**
   * @group Protocols
   */
  case object HeartBeat
    extends ServerToClient with ClientToServer {
    final val HeadByte: Byte = 9
  }
}