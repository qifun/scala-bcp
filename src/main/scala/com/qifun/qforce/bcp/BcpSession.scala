package com.qifun.qforce.bcp

import com.dongxiguo.fastring.Fastring.Implicits._
import java.nio.ByteBuffer
import scala.concurrent.stm._
import scala.annotation.tailrec
import java.nio.channels.AsynchronousSocketChannel
import com.qifun.statelessFuture.util.io.SocketInputStream
import com.qifun.statelessFuture.util.io.SocketWritingQueue
import Bcp._
import java.util.concurrent.ScheduledFuture
import scala.collection.mutable.WrappedArray
import scala.collection.immutable.Queue
import scala.concurrent.stm.Txn
import java.util.concurrent.ScheduledExecutorService
import scala.util.control.Exception.Catcher
import com.qifun.statelessFuture.Future
import com.qifun.qforce.bcp.BcpException.DataTooBig
import java.nio.channels.ClosedChannelException
import java.nio.channels.ShutdownChannelGroupException

private[bcp] object BcpSession {

  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  /**
   * 判断`test`是否在[`low`,`high`)区间。
   *
   * 本函数考虑了整数溢出问题。
   */
  private def between(low: Int, high: Int, test: Int): Boolean = {
    if (low < high) {
      test >= low && test < high
    } else if (low > high) {
      test >= low || test < high
    } else {
      false
    }
  }

  private object IdSet {

    @tailrec
    private def compat(lowId: Int, highId: Int, ids: Set[Int]): IdSet = {
      if (ids(lowId)) {
        compat(lowId + 1, highId, ids - lowId)
      } else {
        new IdSet(lowId, highId, ids)
      }
    }

    def empty = new IdSet(0, 0, Set.empty)

    /**
     * 最新的数据包和最旧的未确认数据包最多相差多少。
     */
    private final val MaxUnconfirmedIds = 1024

  }

  final case class IdSet(lowId: Int, highId: Int, ids: Set[Int]) {

    import IdSet._

    final def +(id: Int) = {
      if (between(lowId, highId, id)) {
        IdSet.compat(lowId, highId, ids + id)
      } else if (between(lowId, lowId + MaxUnconfirmedIds, id)) {
        IdSet.compat(lowId, id + 1, ids + id)
      } else {
        this
      }
    }

    final def isReceived(id: Int) = {
      if (between(lowId, highId, id)) {
        ids.contains(id)
      } else if (between(highId, lowId + MaxUnconfirmedIds, id)) {
        false
      } else {
        true
      }
    }

    final def allReceivedBelow(id: Int): Boolean = {
      ids.isEmpty && lowId == id && highId == id
    }

  }

  private type HeartBeatRunnable = Runnable

  private[bcp] class Stream(override protected val socket: AsynchronousSocketChannel)
    extends SocketInputStream with SocketWritingQueue with HeartBeatRunnable {

    /**
     * For HeartBeatRunnable
     */
    override final def run() {
      logger.finest("sending hartbeat")
      BcpIo.enqueue(this, HeartBeat)
      super.flush()
    }

    val heartBeatTimer = Ref.make[HeartBeatTimer]

    override protected final def readingTimeout = ReadingTimeout

    override protected final def writingTimeout = WritingTimeout

  }

  private type HeartBeatTimer = ScheduledFuture[_]

  private[bcp]type BoxedSessionId = WrappedArray[Byte]

  private[bcp] class Connection[Stream <: BcpSession.Stream] {

    val stream = Ref.make[Stream]

    val finishIdReceived = Ref[Option[Int]](None)

    val isFinishSent = Ref(false)

    /**
     * 收到了多少个[[Data]]
     */
    val numDataReceived = Ref(0)

    val receiveIdSet = Ref[IdSet](IdSet.empty)

    /**
     * 发送了多少个[[Data]]
     */
    val numDataSent = Ref(0)

    /**
     * 收到了多少个用于[[Data]]的[[Acknowledge]]
     */
    val numAcknowledgeReceivedForData = Ref(0)

    /**
     * 已经发送但还没有收到对应的[[Acknowledge]]的数据
     */
    val unconfirmedPackets = Ref(Queue.empty[AcknowledgeRequired])

  }

  private type LastUnconfirmedEnqueueTime = Long

  private[bcp] final val AllConfirmed = Long.MaxValue

  private type SendingConnectionQueue[C <: Connection[_]] = scala.collection.immutable.SortedMap[LastUnconfirmedEnqueueTime, Set[C]]

  final case class PacketQueue(length: Int = 0, queue: Queue[AcknowledgeRequired] = Queue.empty)

}

trait BcpSession[Stream >: Null <: BcpSession.Stream, Connection <: BcpSession.Connection[Stream]] {

  import BcpSession.SendingConnectionQueue
  import BcpSession.PacketQueue
  import BcpSession.logger
  import BcpSession.appender
  import BcpSession.formatter
  import BcpSession.between
  import BcpSession.AllConfirmed
  import scala.collection.immutable.Map

  private type SendingConnectionQueue = BcpSession.SendingConnectionQueue[Connection]

  private[bcp] def newConnection: Connection

  private def newSendingConnectionQueue: SendingConnectionQueue = {
    scala.collection.immutable.SortedMap.empty(Ordering.Long.reverse)
  }

  private[bcp] def internalExecutor: ScheduledExecutorService

  /**
   * 本事件触发时，本[[BcpSession]]的所有TCP连接都已经断线。
   *
   * 即使所有TCP连接都已经断线，本[[BcpSession]]占用的资源仍然不会释放。
   * 因而仍然可以调用[[send]]向本[[BcpSession]]发送数据，但这些数据要等到客户端重连时才会真正发出。
   *
   * 建议在长时间掉线后调用[[shutDown]]释放本[[BcpSession]]占用的资源。
   */
  protected def unavailable()

  /**
   * 本事件触发时，本[[BcpSession]]存在至少一个可用的TCP连接。
   */
  protected def available()

  /**
   * 本事件触发时，表明连接已经断开，这可能是由对端发起的，也可能是本地调用[[shutDown]]的后果。
   */
  protected def shutedDown()

  private[bcp] def release()(implicit txn: InTxn)

  protected def received(pack: ByteBuffer*)

  private val lastConnectionId = Ref(0)

  /**
   * 当前连接，包括尚未关闭的连接和已经关闭但数据尚未全部确认的连接。
   *
   * @note 只有[[Connection.isFinishReceived]]和[[Connection.isFinishSent]]都为`true`，
   * 且[[Connection.unconfirmedPackets]]为空，
   * 才会把[[Connection]]从[[connections]]中移除。
   */
  protected val connections = TMap.empty[Int, Connection]

  private[bcp] val sendingQueue: Ref[Either[PacketQueue, SendingConnectionQueue]] = {
    Ref(Left(PacketQueue()))
  }

  private def addOpenConnection(connection: Connection)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(sendingConnectionQueue) => {
        logger.fine("Before add open connection, sendingQueue: " + sendingQueue())
        sendingConnectionQueue.get(AllConfirmed) match {
          case Some(openConnections) =>
            sendingQueue() = Right(sendingConnectionQueue + (AllConfirmed -> (openConnections + connection)))
          case None =>
            sendingQueue() = Right(sendingConnectionQueue + (AllConfirmed -> Set(connection)))
        }
        logger.fine("After add open connection, sendingQueue: " + sendingQueue())
      }
      case Left(PacketQueue(queueLength, packQueue)) => {
        val stream = connection.stream()
        for (pack <- packQueue) {
          Txn.afterCommit(_ => BcpIo.enqueue(stream, pack))
          connection.unconfirmedPackets.transform(_.enqueue(pack))
        }
        Txn.afterCommit(_ => stream.flush())
        Txn.afterCommit(_ => available())
        if (packQueue.isEmpty) {
          sendingQueue() = Right(newSendingConnectionQueue + (AllConfirmed -> Set(connection)))
        } else {
          sendingQueue() = Right(newSendingConnectionQueue + (System.currentTimeMillis -> Set(connection)))
          busy((for ((connId, conn) <- connections if conn == connection) yield connId).head, connection)
        }
      }
    }
  }

  private def removeOpenConnection(connection: Connection)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(sendingConnectionQueue) =>
        sendingConnectionQueue find { _._2.contains(connection) } match {
          case Some((time, openConnections)) if openConnections.size == 1 =>
            val newSendingConnctionQueue = sendingConnectionQueue - time
            if (newSendingConnctionQueue.isEmpty) {
              sendingQueue() = Left(PacketQueue())
              Txn.afterCommit(_ => unavailable())
            } else {
              sendingQueue() = Right(newSendingConnctionQueue)
            }
          case Some((time, openConnections)) =>
            val newOpenConnections = openConnections - connection
            sendingQueue() = Right(sendingConnectionQueue + (time -> newOpenConnections))
        }
      case left: Left[_, _] =>
    }
  }

  /**
   * 要么立即发送，要么忽略
   */
  private def trySend(newPack: Packet)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(sendingConnectionQueue) =>
        {
          logger.fine("Before try send data, sendingQueue: " + sendingQueue())
          val (time, openConnections) = sendingConnectionQueue.head
          val (firstOpenConnection, restOpenConections) = openConnections.splitAt(1)
          val connection = firstOpenConnection.head
          val stream = connection.stream()
          Txn.afterCommit { _ =>
            BcpIo.enqueue(stream, newPack)
            stream.flush()
          }
          resetHeartBeatTimer(stream)
          val currentTimeMillis = System.currentTimeMillis
          val currentConnections = sendingConnectionQueue.getOrElse(currentTimeMillis, Set[Connection]())
          if (restOpenConections.isEmpty) {
            sendingQueue() = Right(sendingConnectionQueue.tail + (currentTimeMillis -> (currentConnections + connection)))
          } else {
            sendingQueue() = Right(sendingConnectionQueue.tail + (time -> restOpenConections) + (currentTimeMillis -> (currentConnections + connection)))
          }
        }
        logger.fine("After try send data, sendingQueue: " + sendingQueue())
      case left: Left[_, _] =>
    }
  }

  /**
   * 从所有可用连接中轮流发送
   */
  private def enqueue(newPack: AcknowledgeRequired)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(sendingConnectionQueue) =>
        {
          logger.fine("Before enqueue data, sendingQueue: " + sendingQueue())
          val (time, openConnections) = sendingConnectionQueue.head
          val (firstOpenConnection, restOpenConnections) = openConnections.splitAt(1)
          val connection = firstOpenConnection.head
          val stream = connection.stream()
          busy((for ((connId, conn) <- connections if conn == connection) yield connId).head, connection)
          Txn.afterCommit { _ =>
            BcpIo.enqueue(stream, newPack)
            stream.flush()
          }
          connection.unconfirmedPackets.transform(_.enqueue(newPack))
          resetHeartBeatTimer(stream)
          val currentTimeMillis = System.currentTimeMillis
          val currentConnections = sendingConnectionQueue.getOrElse(currentTimeMillis, Set[Connection]())
          if (restOpenConnections.isEmpty) {
            sendingQueue() = Right(sendingConnectionQueue.tail + (currentTimeMillis -> (currentConnections + connection)))
          } else {
            sendingQueue() = Right(sendingConnectionQueue.tail + (time -> restOpenConnections) + (currentTimeMillis -> (currentConnections + connection)))
          }
        }
        logger.fine("After enqueue data, sendingQueue: " + sendingQueue())
      case Left(PacketQueue(queueLength, packQueue)) => {
        if (queueLength >= MaxOfflinePack) {
          throw new BcpException.SendingQueueIsFull
        } else {
          sendingQueue() = Left(PacketQueue(queueLength + 1, packQueue.enqueue(newPack)))
        }
      }
    }
  }

  private def enqueueFinish(connection: Connection)(implicit txn: InTxn) {
    connection.unconfirmedPackets.transform(_.enqueue(Finish))
    val stream = connection.stream()
    Txn.afterCommit { _ =>
      BcpIo.enqueue(stream, Finish)
      stream.flush()
    }
    connection.isFinishSent() = true
  }

  private[bcp] final def finishConnection(connectionId: Int, connection: Connection)(implicit txn: InTxn) {
    enqueueFinish(connection)
    removeOpenConnection(connection)
  }

  private def checkConnectionFinish(connectionId: Int, connection: Connection)(implicit txn: InTxn) {
    val isConnectionFinish =
      connection.isFinishSent() &&
        connection.finishIdReceived().exists(connection.receiveIdSet().allReceivedBelow) &&
        connection.unconfirmedPackets().isEmpty
    if (isConnectionFinish) { // 所有外出数据都已经发送并确认，所有外来数据都已经收到并确认
      connections.remove(connectionId)
    }
  }

  private def dataReceived(
    connectionId: Int,
    connection: Connection,
    packId: Int,
    buffer: Seq[ByteBuffer])(
      implicit txn: InTxn) {
    val idSet = connection.receiveIdSet()
    if (idSet.isReceived(packId)) {
      // 已经收过了，直接忽略。
    } else {
      Txn.afterCommit(_ => received(buffer: _*))
      connection.receiveIdSet() = idSet + packId
      checkConnectionFinish(connectionId, connection)
    }
  }

  private def checkShutDown()(implicit txn: InTxn) {
    trySend(ShutDown)
    release()
    sendingQueue() match {
      case Right(sendingConnectionQueue) => {
        for (openConnections <- sendingConnectionQueue.values; connection <- openConnections) {
          close((for ((connId, conn) <- connections if conn == connection) yield connId).head, connection)
          val stream = connection.stream()
          connection.stream() = null
          assert(stream != null)
          // 直接关闭所有TCP连接，对端要是还没收到ShutDownInput的Acknowledge的话，只能靠TCP的CLOSE_WAIT机制了。
          Txn.afterCommit(_ => {
            stream.shutDown()
          })
        }
        sendingQueue() = Left(PacketQueue())
      }
      case left: Left[_, _] =>
    }
    Txn.afterCommit(_ => shutedDown())
  }

  private def retransmissionFinishReceived(
    connectionId: Int,
    connection: Connection,
    packId: Int)(implicit txn: InTxn) {
    connection.numDataReceived() = packId + 1
    connection.finishIdReceived() match {
      case None => {
        connection.finishIdReceived() = Some(packId)
        checkConnectionFinish(connectionId, connection)
        // 无需触发事件通知用户，结束的只是一个连接，而不是整个Session
      }
      case Some(originalPackId) => {
        throw new BcpException.AlreadyReceivedFinish
      }
    }
  }

  private def cleanUp(connectionId: Int, connection: Connection, stream: Stream)(implicit txn: InTxn) {
    removeOpenConnection(connection)
    connection.stream() = null
    val heartBeatTimer = stream.heartBeatTimer()
    stream.heartBeatTimer() = null
    Txn.afterCommit(_ => heartBeatTimer.cancel(false))
    connection.unconfirmedPackets().foldLeft(connection.numAcknowledgeReceivedForData()) {
      case (packId, Data(buffer)) => {
        enqueue(RetransmissionData(connectionId, packId, buffer))
        packId + 1
      }
      case (packId, Finish) => {
        RetransmissionFinish(connectionId, packId)
        enqueue(RetransmissionFinish(connectionId, packId))
        packId + 1
      }
      case (nextPackId, pack) => {
        enqueue(pack)
        nextPackId
      }
    }
    connection.unconfirmedPackets() = Queue.empty
    checkConnectionFinish(connectionId, connection)

  }

  private def startReceive(
    connectionId: Int,
    connection: Connection,
    stream: Stream) {
    val receiveFuture = Future {
      BcpIo.receive(stream).await match {
        case HeartBeat => {
          logger.finest("received heart beat")
          startReceive(connectionId, connection, stream)
        }
        case Data(buffer) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            val packId = connection.numDataReceived()
            connection.numDataReceived() = packId + 1
            dataReceived(connectionId, connection, packId, buffer)
          }
          startReceive(connectionId, connection, stream)
          stream.flush()
        }
        case RetransmissionData(dataConnectionId, packId, data) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            connections.get(dataConnectionId) match {
              case Some(dataConnection) => {
                dataReceived(dataConnectionId, dataConnection, packId, data)
              }
              case None => {
                val lastConnectionId = this.lastConnectionId()
                if (between(lastConnectionId, lastConnectionId + MaxConnectionsPerSession, dataConnectionId)) {
                  // 在成功建立连接以前先收到重传的数据，这表示原连接在BCP握手阶段卡住了
                  val c = newConnection
                  connections(dataConnectionId) = c
                  dataReceived(dataConnectionId, c, packId, data)
                } else {
                  // 原连接先前已经接收过所有数据并关闭了，可以安全忽略数据
                }
              }
            }
          }
          startReceive(connectionId, connection, stream)
          stream.flush()
        }
        case Acknowledge => {
          atomic { implicit txn =>
            val (originalPack, queue) = connection.unconfirmedPackets().dequeue
            connection.unconfirmedPackets() = queue
            if (queue.isEmpty) {
              sendingQueue() match {
                case Right(sendingConnectionQueue) =>
                  logger.fine("Before Acknowledge, sendingQueue: " + sendingConnectionQueue)
                  val (time, openConnections) = (sendingConnectionQueue.find({ _._2.contains(connection) })).get
                  val newOpenConnections = openConnections - connection
                  val allConfirmedConnections = sendingConnectionQueue.getOrElse(AllConfirmed, Set[Connection]())
                  if (newOpenConnections.isEmpty) {
                    sendingQueue() = Right(sendingConnectionQueue - time + (AllConfirmed -> (allConfirmedConnections + connection)))
                  } else {
                    sendingQueue() = Right(sendingConnectionQueue + (time -> newOpenConnections) + (AllConfirmed -> (allConfirmedConnections + connection)))
                  }
                  logger.fine("After Acknowledge, sendingQueue" + sendingQueue())
                case left: Left[_, _] =>
              }
              idle(connectionId, connection)
            }
            originalPack match {
              case Data(buffer) => {
                connection.numAcknowledgeReceivedForData() += 1
              }
              case RetransmissionData(_, _, _) | Finish | RetransmissionFinish(_, _) => {
                // 简单移出重传队列即可，不用任何额外操作
              }
            }
            checkConnectionFinish(connectionId, connection)
          }
          startReceive(connectionId, connection, stream)
        }
        case Finish => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            if (!connection.isFinishSent()) {
              enqueueFinish(connection)
            }
            val packId = connection.numDataReceived()
            connection.finishIdReceived() = Some(packId)
            cleanUp(connectionId, connection, stream)
          }
          stream.shutDown()
        }
        case RetransmissionFinish(finishConnectionId, packId) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            connections.get(finishConnectionId) match {
              case Some(finishConnection) => {
                retransmissionFinishReceived(finishConnectionId, finishConnection, packId)
              }
              case None => {
                val lastConnectionId = this.lastConnectionId()
                if (between(lastConnectionId, lastConnectionId + MaxConnectionsPerSession, connectionId)) {
                  // 在成功建立连接以前先收到重传的数据，这表示原连接在BCP握手阶段卡住了
                  val c = newConnection
                  connections(finishConnectionId) = c
                  retransmissionFinishReceived(finishConnectionId, c, packId)
                } else {
                  // 原连接先前已经接收过所有数据并关闭了，可以安全忽略数据
                }
              }
            }
          }
          startReceive(connectionId, connection, stream)
          stream.flush()
        }
        case ShutDown => {
          atomic { implicit txn =>
            checkShutDown()
          }
        }
        case Renew => {
          atomic { implicit txn =>
            sendingQueue() match {
              case Right(sendingConnectionQueue) => {
                for (openConnections <- sendingConnectionQueue.values; originalConnection <- openConnections) {
                  if (originalConnection != connection) {
                    val stream = originalConnection.stream()
                    stream.interrupt()
                    val heartBeatTimer = stream.heartBeatTimer()
                    Txn.afterCommit(_ => heartBeatTimer.cancel(false))
                    stream.heartBeatTimer() = null
                    originalConnection.stream() = null
                  }
                }
              }
              case _: Left[_, _] =>
            }
            sendingQueue() = Right(newSendingConnectionQueue)
            connections.clear()
            connections(connectionId) = connection
          }
        }
      }
    }

    implicit def catcher: Catcher[Unit] = {
      case e @ (_: ClosedChannelException | _: ShutdownChannelGroupException) => {
        // 由于自己主动关闭连接而触发异常
        logger.fine(e)
        atomic { implicit txn =>
          cleanUp(connectionId, connection, stream)
        }
      }
      case e: Exception => {
        // 由于协议错误、对端断开连接、网络繁忙，而触发异常
        logger.warning(e)
        stream.interrupt()
        atomic { implicit txn =>
          cleanUp(connectionId, connection, stream)
        }
      }
    }
    for (_ <- receiveFuture) {
      logger.fine("The packet is processed successfully.")
    }
  }

  private def resetHeartBeatTimer(stream: Stream)(implicit txn: InTxn) {
    val oldTimer = stream.heartBeatTimer()
    Txn.afterCommit(_ => oldTimer.cancel(false))
    val newTimer =
      internalExecutor.scheduleWithFixedDelay(
        stream,
        HeartBeatDelay.length,
        HeartBeatDelay.length,
        HeartBeatDelay.unit)
    Txn.afterRollback(_ => newTimer.cancel(false))
    stream.heartBeatTimer() = newTimer
  }

  final def shutDown() {
    atomic { implicit txn: InTxn =>
      checkShutDown()
    }
  }

  final def send(buffer: ByteBuffer*) {
    atomic { implicit txn =>
      enqueue(Data(buffer))
    }
  }

  /** 当有数据发出时触发本事件 */
  private[bcp] def busy(connectionId: Int, connection: Connection)(implicit txn: InTxn): Unit

  /** 当所有的数据都收到[[Bcp.Acknowledge]]时触发本事件 */
  private[bcp] def idle(connectionId: Int, connection: Connection)(implicit txn: InTxn): Unit

  private[bcp] def close(connectionId: Int, connection: Connection)(implicit txn: InTxn): Unit

  private[bcp] final def addStream(connectionId: Int, stream: Stream)(implicit txn: InTxn) {
    if (connections.size >= MaxConnectionsPerSession) {
      stream.interrupt()
    }
    val connection = connections.getOrElseUpdate(connectionId, newConnection)
    lastConnectionId() = connectionId
    if (connection.stream() == null) {
      connection.stream() = stream
      addOpenConnection(connection)
      Txn.afterCommit(_ => startReceive(connectionId, connection, stream))
      val timer =
        internalExecutor.scheduleWithFixedDelay(
          stream,
          HeartBeatDelay.length,
          HeartBeatDelay.length,
          HeartBeatDelay.unit)
      Txn.afterRollback(_ => timer.cancel(false))
      stream.heartBeatTimer() = timer
    } else {
      logger.fine(fast"A client atempted to reuse existed connectionId. I rejected it.")
      stream.interrupt()
    }
  }

}