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

private[bcp] object BcpSession {
  private implicit val (logger, formater, appender) = ZeroLoggerFactory.newLogger(this)

  private class IdSetIsFullException extends Exception

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

    object NonEmpty {
      @tailrec
      private def compat(lowId: Int, highId: Int, ids: Set[Int]): NonEmpty = {
        if (ids(lowId)) {
          compat(lowId + 1, highId, ids - lowId)
        } else {
          new NonEmpty(lowId, highId, ids)
        }
      }
    }

    final case class NonEmpty(lowId: Int, highId: Int, ids: Set[Int]) extends IdSet {
      import NonEmpty._

      @throws(classOf[IdSetIsFullException])
      override final def +(id: Int) = {
        if (between(lowId, highId, id)) {
          compat(lowId, highId, ids + id)
        } else if (between(highId, highId + 1024, id)) {
          if (between(lowId, lowId + 2048, id)) {
            throw new IdSetIsFullException
          } else {
            new NonEmpty(lowId, id, ids + id)
          }
        } else {
          this
        }
      }

      override final def contains(id: Int) = {
        if (between(lowId, highId, id)) {
          ids.contains(id)
        } else if (between(highId, highId + 1024, id)) {
          false
        } else {
          true
        }
      }

      override final def allReceivedBelow(id: Int): Boolean = {
        ids.isEmpty && lowId == id && highId == id
      }

    }

    case object Empty extends IdSet {

      @throws(classOf[IdSetIsFullException])
      override final def +(id: Int) = new NonEmpty(id + 1, id + 1, Set.empty[Int])

      override final def contains(id: Int) = false

      override final def allReceivedBelow(id: Int) = true

    }
  }

  private sealed abstract class IdSet {
    def +(id: Int): IdSet
    def contains(id: Int): Boolean
    def allReceivedBelow(id: Int): Boolean
  }

  private type HeartBeatRunnable = Runnable

  private[bcp] final class Stream(override protected val socket: AsynchronousSocketChannel)
    extends SocketInputStream with SocketWritingQueue with HeartBeatRunnable {

    /**
     * For HeartBeatRunnable
     */
    override final def run() {
      BcpIo.enqueue(this, HeartBeat)
      super.flush()
    }
    val heartBeatTimer = Ref.make[HeartBeatTimer]

    override protected final def readingTimeout = ReadingTimeout

    override protected final def writingTimeout = WritingTimeout

  }

  private type HeartBeatTimer = ScheduledFuture[_]

  private[bcp]type BoxedSessionId = WrappedArray[Byte]

  private final class Connection {

    val stream = Ref.make[Stream]

    val finishIdReceived = Ref[Option[Int]](None)

    val isFinishSent = Ref(false)

    /**
     * 收到了多少个[[Data]]
     */
    val numDataReceived = Ref(0)

    val receiveIdSet = Ref[IdSet](IdSet.Empty)

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
    val unconfirmedPack = Ref(Queue.empty[AcknowledgeRequired])

  }

  private final case class SendingConnectionQueue(
    val openConnections: Set[Connection] = Set.empty[Connection],
    val availableConnections: Set[Connection] = Set.empty[Connection])

  final case class PacketQueue(length: Int = 0, queue: Queue[AcknowledgeRequired] = Queue.empty)

}

trait BcpSession {

  import BcpSession._

  private[bcp] def internalExecutor: ScheduledExecutorService

  /**
   * 每一次触发表示与对端建立了一次新的会话。
   */
  protected def open()

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
   * 且[[Connection.unconfirmedPack]]为空，
   * 才会把[[Connection]]从[[connections]]中移除。
   */
  private val connections = TMap.empty[Int, Connection]

  private val sendingQueue: Ref[Either[PacketQueue, SendingConnectionQueue]] = {
    Ref(Right(SendingConnectionQueue()))
  }

  private def addOpenConnection(connection: Connection)(implicit txn: InTxn) {
    sendingQueue.transform {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
        Right(SendingConnectionQueue(openConnections + connection, availableConnections + connection))
      }
      case Left(PacketQueue(queueLength, packQueue)) => {
        val stream = connection.stream()
        for (pack <- packQueue) {
          Txn.afterCommit(_ => BcpIo.enqueue(stream, pack))
          connection.unconfirmedPack.transform(_.enqueue(pack))
        }
        resetHeartBeatTimer(stream)
        Txn.afterCommit(_ => stream.flush())
        Txn.afterCommit(_ => available())
        Right(SendingConnectionQueue(Set(connection), Set(connection)))
      }
    }
  }

  private def removeOpenConnection(connection: Connection)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) =>
        if (openConnections.head == connection && openConnections.tail.isEmpty) {
          sendingQueue() = Left(PacketQueue())
          Txn.afterCommit(_ => unavailable())
        } else {
          val newQueue = SendingConnectionQueue(openConnections - connection, availableConnections - connection)
          sendingQueue() = Right(newQueue)
        }
      case left: Left[_, _] =>
    }

  }

  /**
   * 要么立即发送，要么忽略
   */
  private def trySend(newPack: Packet)(implicit txn: InTxn) {
    sendingQueue() match {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
        def consume(openConnections: Set[Connection], availableConnections: Set[Connection]) {
          val (first, rest) = availableConnections.splitAt(1)
          val connection = first.head
          val stream = connection.stream()
          Txn.afterCommit { _ =>
            BcpIo.enqueue(stream, newPack)
            stream.flush()
          }
          resetHeartBeatTimer(stream)
          sendingQueue() = Right(SendingConnectionQueue(openConnections, rest))
        }
        if (availableConnections.isEmpty) {
          consume(openConnections, openConnections)
        } else {
          consume(openConnections, availableConnections)
        }
      }
      case left: Left[_, _] =>
    }
  }

  /**
   * 从所有可用连接中轮流发送
   */
  private def enqueue(newPack: AcknowledgeRequired)(implicit txn: InTxn) {
    sendingQueue.transform {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
        def consume(openConnections: Set[Connection], availableConnections: Set[Connection]) = {
          val (first, rest) = availableConnections.splitAt(1)
          val connection = first.head
          val stream = connection.stream()
          Txn.afterCommit { _ =>
            BcpIo.enqueue(stream, newPack)
            stream.flush()
          }
          connection.unconfirmedPack.transform(_.enqueue(newPack))
          resetHeartBeatTimer(stream)
          Right(SendingConnectionQueue(openConnections, rest))
        }
        if (availableConnections.isEmpty) {
          consume(openConnections, openConnections)
        } else {
          consume(openConnections, availableConnections)
        }
      }
      case Left(PacketQueue(queueLength, packQueue)) => {
        if (queueLength >= MaxOfflinePack) {
          throw new BcpException.SendingQueueIsFull
        } else {
          Left(PacketQueue(queueLength + 1, packQueue.enqueue(newPack)))
        }
      }
    }
  }

  private def checkConnectionFinish(connectionId: Int, connection: Connection)(implicit txn: InTxn) {
    val isConnectionFinish =
      connection.isFinishSent() &&
        connection.finishIdReceived().exists(connection.receiveIdSet().allReceivedBelow) &&
        connection.unconfirmedPack().isEmpty
    if (isConnectionFinish) { // 所有外出数据都已经发送并确认，所有外来数据都已经收到并确认
      removeOpenConnection(connection)
      connections.remove(connectionId)
      val connectionStream = connection.stream()
      connection.stream() = null
      val heartBeatTimer = connectionStream.heartBeatTimer()
      connectionStream.heartBeatTimer() = null
      Txn.afterCommit { _ =>
        connectionStream.interrupt()
        heartBeatTimer.cancel(false)
      }
    }
  }

  private def dataReceived(
    connectionId: Int,
    connection: Connection,
    packId: Int,
    buffer: Seq[ByteBuffer])(
      implicit txn: InTxn) {
    val idSet = connection.receiveIdSet()
    if (idSet.contains(packId)) {
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
      case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
        for (connection <- openConnections) {
          val stream = connection.stream()
          connection.stream() = null
          assert(stream != null)
          val heartBeatTimer = stream.heartBeatTimer()
          // 直接关闭所有TCP连接，对端要是还没收到ShutDownInput的Acknowledge的话，只能靠TCP的CLOSE_WAIT机制了。
          Txn.afterCommit(_ => stream.shutDown())
        }
        sendingQueue() = Left(PacketQueue())
      }
      case left: Left[_, _] =>
    }
    Txn.afterCommit(_ => shutedDown())

  }

  private def finishReceived(

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
        assert(originalPackId == packId)
      }
    }
  }

  private def startReceive(

    connectionId: Int,
    connection: Connection,
    stream: Stream) {
    val receiveFuture = Future {
      BcpIo.receive(stream).await match {
        case HeartBeat => {
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
                  val newConnection = new Connection
                  connections(dataConnectionId) = newConnection
                  dataReceived(dataConnectionId, newConnection, packId, data)
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
            val (originalPack, queue) = connection.unconfirmedPack().dequeue
            connection.unconfirmedPack() = queue
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
            resetHeartBeatTimer(stream)
            val packId = connection.numDataReceived()
            finishReceived(connectionId, connection, packId)
          }
          startReceive(connectionId, connection, stream)
          stream.flush()
        }
        case RetransmissionFinish(finishConnectionId, packId) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            connections.get(finishConnectionId) match {
              case Some(finishConnection) => {
                finishReceived(finishConnectionId, finishConnection, packId)
              }
              case None => {
                val lastConnectionId = this.lastConnectionId()
                if (between(lastConnectionId, lastConnectionId + MaxConnectionsPerSession, connectionId)) {
                  // 在成功建立连接以前先收到重传的数据，这表示原连接在BCP握手阶段卡住了
                  val newConnection = new Connection
                  connections(finishConnectionId) = newConnection
                  finishReceived(finishConnectionId, newConnection, packId)
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
              case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
                for (originalConnection <- openConnections) {
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
            sendingQueue() = Right(SendingConnectionQueue(Set(connection), Set(connection)))
            connections.clear()
            connections(connectionId) = connection
          }
        }
      }
    }

    implicit def catcher: Catcher[Unit] = {
      case e: Exception => {
        logger.warning(e)
        stream.interrupt()
        atomic { implicit txn =>
          removeOpenConnection(connection)
          connection.stream() = null
          val heartBeatTimer = stream.heartBeatTimer()
          stream.heartBeatTimer() = null
          Txn.afterCommit(_ => heartBeatTimer.cancel(false))
          connection.unconfirmedPack().foldLeft(connection.numAcknowledgeReceivedForData()) {
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
          connection.unconfirmedPack() = Queue.empty
          checkConnectionFinish(connectionId, connection)
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

  private[bcp] final def addStream(connectionId: Int, stream: Stream)(implicit txn: InTxn) {
    if (connections.size >= MaxConnectionsPerSession) {
      stream.interrupt()
    }
    val connection = connections.getOrElseUpdate(connectionId, new Connection)
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