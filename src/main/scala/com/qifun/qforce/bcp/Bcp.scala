package com.qifun.qforce.bcp
import scala.concurrent.duration._
import java.nio.ByteBuffer

/**
 * BCP协议相关的数据结构和常量
 *
 * BCP（Brutal Control Protocol，残暴控制协议）是基于TCP实现的用户层传输协议。特性如下：
 *
 *  1. 基于连接
 *  1. 可靠，低延时
 *  1. 以数据包为单位，没有流
 *  1. 乱序数据包，不保证接收顺序与发送顺序一致
 *
 * <h3>BCP vs. TCP</h3>
 *
 * BCP和普通TCP功能相似，重新实现了TCP的包确认重发机制。
 *
 * 当网络条件不好，丢包频繁，TCP重传间隔时间会变得很长，从而基本不可用。
 * 这种情况下，BCP会暴力杀死底层TCP连接，强制重建TCP连接，暴力重发数据，避免丢失数据并减少延时。
 *
 * BCP和TCP都属于面向连接会话的协议。
 * 但另一方面，BCP数据不是TCP那样的流，而以[[Packet]]为单位。
 * BCP协议不保证接收方收取[[Packet]]的顺序和发送方发出[[Packet]]的顺序一致。
 * 这是因为，一条BCP会话，可能会对应多达[[MaxConnectionsPerSession]]个底层TCP连接，
 * 而这几条底层TCP连接的延时可能并不相同。
 *
 * <h3>BCP会话的一生</h3>
 *
 *  1. 客户端随机生成[[NumBytesSessionId]]字节的会话ID。
 *  1. 客户端发起TCP连接。
 *  1. TCP连接成功建立后，客户端发送[[ConnectionHead]]，
 *     其中包括先前生成的会话ID和varint格式的底层连接ID。
 *     每条会话的底层连接ID都从0开始，
 *     每当客户端发起新TCP连接时，底层连接ID递增1。
 *  1. 客户端或服务端发送[[Data]]包，向对端发送用户数据。
 *  1. 每发一个[[Data]]，对端都回应[[Acknowledge]]。
 *  1. 如果客户端或服务端连续[[HeartBeatDelay]]时间，
 *     都没有[[Packet]]需要发送，那么发送一个[[HeartBeat]]到对端。
 *  1. 如果客户端或服务端连续[[ReadingTimeout]]都收不到任何数据，
 *     就认为这个TCP连接已经没救了，就关闭这个TCP连接。
 *  1. 当客户端发送一个包，并超过[[BusyTimeout]]时间收不到对端都回应的[[Acknowledge]]时，
 *     就认为已有底层TCP连接速度太慢，客户端可以发起新的TCP连接，
 *     把一部分[[Data]]放到新的TCP连接上发送。
 *  1. 新增TCP连接与首个TCP一样，仍然属于同一会话，
 *     客户端也需要在连接建立后发送[[ConnectionHead]]。
 *     但此处的会话ID应当重用原有的会话ID，不要生成新ID。
 *  1. 如果客户端发现某一会话中的TCP连接超过[[IdleTimeout]]时间没有发送任何数据，
 *     且该连接不是最后一条连接，那么客户端可以向服务端发送[[Finish]]，关掉这条TCP连接。
 *  1. 服务器收到[[Finish]]时，应当回复[[Acknowledge]]，然后关掉底层连接。
 *  1. 如果一个底层TCP连接因为超时或者别的原因异常中断，
 *     那么每一端都必须在同一会话的其他TCP连接上，
 *     重发那些尚未收到对应[[Acknowledge]]的[[Data]]和[[Finish]]。
 *     重发的[[RetransmissionData]]和[[RetransmissionFinish]]包中，
 *     包含原先连接ID和原先包ID。
 *     包ID是指原包的序号，从零开始计算，
 *     原连接每发送一个[[Data]]或[[Finish]]，序号就递增1。
 *  1. 当客户端和服务端希望结束BCP会话时，向对端发送[[ShutDown]]，
 *     然后清除该会话相关的所有数据。
 *  1. 对端收到[[ShutDown]]后，无须回复，直接清除会话相关的所有数据。
 *
 * 此外，客户端可以把会话ID记录在文件中。
 * 如果应用崩溃，用户在短时间内重连，
 * 客户端可以使用先前保存的会话ID，而不需要重新生成会话ID。
 * 这种情况下，客户端应当在建立连接后立即发送[[Renew]]给服务端，
 * 服务端会放弃重发原会话尚未成功发送的数据，
 * 但同时会视为会话从未断开，不会清除原会话的服务端数据。
 */
object Bcp {

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
   * 当一个TCP连接空闲多长时间没发送任何数据时，发一个心跳包
   *
   * @group Constants
   */
  final val HeartBeatDelay = 3.seconds

  /**
   * 多长时间收不到任何包，就杀掉TCP连接。
   *
   * @group Constants
   */
  final val ReadingTimeout = 6.seconds

  /**
   * 多长时间发不出数据就杀掉TCP连接。
   *
   * 发数据超时只可能因为TCP缓冲区满，
   * 只有发送超大数据包时，客户端很慢，才会发生这种情况，可能性微乎其微。
   *
   * @group Constants
   */
  final val WritingTimeout = 1.seconds

  /**
   * 连接在busy状态多长时间就新建链接
   *
   * @group Constants
   */
  final val BusyTimeout = 500.milliseconds

  /**
   * 连接在idle状态多长时间就关闭链接
   *
   * @group Constants
   */
  final val IdleTimeout = 10.seconds

  /**
   * Session ID由多少字节构成
   *
   * @group Constants
   */
  final val NumBytesSessionId = 16

  /**
   * 数据包上限是多少字节
   *
   * @group Constants
   */
  final val MaxDataSize = 100000

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
  final case class Data(buffers: Seq[ByteBuffer])
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
  final case class RetransmissionData(connectionId: Int, packId: Int, buffers: Seq[ByteBuffer])
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
  final case class RetransmissionFinish(connectionId: Int, packId: Int)
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

  object ConnectionState extends Enumeration {
    type ConnectionState = Value
    val Idle = Value
    val Busy = Value
    val Slow = Value
  }

}
