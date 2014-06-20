package com.qifun.qforce.bcp.server

import com.dongxiguo.fastring.Fastring.Implicits._
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import scala.annotation.tailrec
import scala.collection.mutable.WrappedArray
import scala.concurrent.duration.DurationInt
import scala.concurrent.stm.InTxn
import scala.concurrent.stm.Ref
import scala.concurrent.stm.TMap
import scala.concurrent.stm.TSet
import scala.concurrent.stm.Txn
import scala.concurrent.stm.atomic
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.util.control.NoStackTrace
import scala.util.control.Exception.Catcher
import com.qifun.statelessFuture.io.SocketInputStream
import com.qifun.statelessFuture.io.SocketWritingQueue
import com.qifun.statelessFuture.Future
import scala.collection.immutable.Queue
import com.qifun.qforce.bcp.server.Bcp._

object BcpServer {

  private implicit val (logger, formater, appender) = ZeroLoggerFactory.newLogger(this)

  private class IdSetIsFullException extends Exception

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

  private final class Stream(override protected final val socket: AsynchronousSocketChannel)
    extends SocketInputStream with SocketWritingQueue with Runnable {

    override final def run() {
      BcpIo.enqueue(this, HeartBeat)
      super.flush()
    }
    val heartBeatTimer = Ref.make[HeartBeatTimer]

    override protected final def readingTimeout = ServerReadingTimeout

    override protected final def writingTimeout = ServerWritingTimeout

  }

  private type HeartBeatTimer = ScheduledFuture[_]

  private type BoxedSessionId = WrappedArray[Byte]

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
    val unconfirmedPack = Ref(Queue.empty[AcknowledgeRequired with ServerToClient])

  }
  //
  //  private sealed trait ShutDownInputState
  //  private object ShutDownInputState {
  //    final case object NotShutDownInput extends ShutDownInputState
  //    final case object InputShutedDown extends ShutDownInputState
  //  }
  //  private sealed trait ShutDownOutputState
  //  private object ShutDownOutputState {
  //    final case object NotShutDownOutput extends ShutDownOutputState
  //    final case object OutputShutedDown extends ShutDownOutputState
  //
  //    /**
  //     * 等待所有已发的数据都收到[[Acknowledge]]
  //     *
  //     * @param numAcknowledgeToReceive 还有多少个[[Acknowledge]]要收。
  //     */
  //    final case class ShutDownOutputWaiting(numAcknowledgeToReceive: Int) extends ShutDownOutputState
  //  }

  private final case class SendingConnectionQueue(
    val openConnections: Set[Connection] = Set.empty[Connection],
    val availableConnections: Set[Connection] = Set.empty[Connection])

  final case class PacketQueue(length: Int = 0, queue: Queue[AcknowledgeRequired with ServerToClient] = Queue.empty)

  trait Session {

    private[BcpServer] final var sessionId: Array[Byte] = null

    private[BcpServer] final val lastConnectionId = Ref(0)

    /**
     * 当前连接，包括尚未关闭的连接和已经关闭但数据尚未全部确认的连接。
     *
     * @note 只有[[Connection.isFinishReceived]]和[[Connection.isFinishSent]]都为`true`，
     * 且[[Connection.unconfirmedPack]]为空，
     * 才会把[[Connection]]从[[connections]]中移除。
     */
    private[BcpServer] final val connections = TMap.empty[Int, Connection]

    private[BcpServer] final val sendingQueue: Ref[Either[PacketQueue, SendingConnectionQueue]] = {
      Ref(Right(SendingConnectionQueue()))
    }

  }
}

import BcpServer._
/**
 * 处理BCP协议的服务器。
 *
 * BCP协议的特性：
 *
 * <ol>
 *   <li>基于连接</li>
 *   <li>可靠，低延时</li>
 *   <li>以数据包为单位，没有流</li>
 *   <li>乱序数据包，不保证接收顺序与发送顺序一致</li>
 * </ol>
 */
abstract class BcpServer[Session <: BcpServer.Session: ClassTag] {
  import BcpServer.appender
  import BcpServer.formater

  protected def executor: ScheduledExecutorService

  /**
   * 每一次触发表示与客户端建立了一次新的会话。
   *
   * 建立一次会话期间，本[[BcpServer]]可能会多次创建[[BcpServer.Session]]，但一定只调用一次[[open]]。
   */
  protected def open(session: Session)

  /**
   * 本事件触发时，`session`的所有TCP连接都已经断线。
   *
   * 你仍然可以调用[[send]]向`session`发送数据，但这些数据要等到客户端重连时才会真正发出。
   */
  protected def unavailable(session: Session)

  /**
   * 本事件触发时，`session`存在至少一个可用的TCP连接。
   */
  protected def available(session: Session)

  /**
   * 本事件触发时，表明连接已经断开，这可能是由对端发起的，也可能是本地调用[[shutDown]]的后果。
   */
  protected def shutedDown(session: Session)

  protected def received(session: Session, pack: ByteBuffer*)

  private val sessions = TMap.empty[BoxedSessionId, Session]

  private def addOpenConnection(session: Session, connection: Connection)(implicit txn: InTxn) {
    session.sendingQueue.transform {
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
        Txn.afterCommit(_ => available(session))
        Right(SendingConnectionQueue(Set(connection), Set(connection)))
      }
    }
  }

  private def removeOpenConnection(session: Session, connection: Connection)(implicit txn: InTxn) {
    session.sendingQueue() match {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) =>
        if (openConnections.head == connection && openConnections.tail.isEmpty) {
          session.sendingQueue() = Left(PacketQueue())
          Txn.afterCommit(_ => unavailable(session))
        } else {
          val newQueue = SendingConnectionQueue(openConnections - connection, availableConnections - connection)
          session.sendingQueue() = Right(newQueue)
        }
      case left: Left[_, _] =>
    }

  }

  /**
   * 要么立即发送，要么忽略
   */
  private def trySend(session: Session, newPack: ServerToClient)(implicit txn: InTxn) {
    session.sendingQueue() match {
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
          session.sendingQueue() = Right(SendingConnectionQueue(openConnections, rest))
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
  private def enqueue(session: Session, newPack: ServerToClient with AcknowledgeRequired)(implicit txn: InTxn) {
    session.sendingQueue.transform {
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

  private def checkConnectionFinish(session: Session, connectionId: Int, connection: Connection)(implicit txn: InTxn) {
    val isConnectionFinish =
      connection.isFinishSent() &&
        connection.finishIdReceived().exists(connection.receiveIdSet().allReceivedBelow) &&
        connection.unconfirmedPack().isEmpty
    if (isConnectionFinish) { // 所有外出数据都已经发送并确认，所有外来数据都已经收到并确认
      removeOpenConnection(session, connection)
      session.connections.remove(connectionId)
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
    session: Session,
    connectionId: Int,
    connection: Connection,
    packId: Int,
    buffer: Seq[ByteBuffer])(
      implicit txn: InTxn) {
    val idSet = connection.receiveIdSet()
    if (idSet.contains(packId)) {
      // 已经收过了，直接忽略。
    } else {
      Txn.afterCommit(_ => received(session, buffer: _*))
      connection.receiveIdSet() = idSet + packId
      checkConnectionFinish(session, connectionId, connection)
    }
  }

  private def checkShutDown(session: Session)(implicit txn: InTxn) {
    trySend(session, ShutDown)
    val removedSessionOption = sessions.remove(session.sessionId)
    assert(removedSessionOption == Some(session))
    session.sendingQueue() match {
      case Right(SendingConnectionQueue(openConnections, availableConnections)) => {
        for (connection <- openConnections) {
          val stream = connection.stream()
          connection.stream() = null
          assert(stream != null)
          val heartBeatTimer = stream.heartBeatTimer()
          // 直接关闭所有TCP连接，对端要是还没收到ShutDownInput的Acknowledge的话，只能靠TCP的CLOSE_WAIT机制了。
          Txn.afterCommit(_ => stream.shutDown())
        }
        session.sendingQueue() = Left(PacketQueue())
      }
      case left: Left[_, _] =>
    }
    Txn.afterCommit(_ => shutedDown(session))

  }

  private def finishReceived(
    session: Session,
    connectionId: Int,
    connection: Connection,
    packId: Int)(implicit txn: InTxn) {
    connection.numDataReceived() = packId + 1
    connection.finishIdReceived() match {
      case None => {
        connection.finishIdReceived() = Some(packId)
        checkConnectionFinish(session, connectionId, connection)
        // 无需触发事件通知用户，结束的只是一个连接，而不是整个Session
      }
      case Some(originalPackId) => {
        assert(originalPackId == packId)
      }
    }
  }

  private def startReceive(
    session: Session,
    connectionId: Int,
    connection: Connection,
    stream: Stream) {
    implicit def catcher: Catcher[Unit] = {
      case e: Exception => {
        stream.interrupt()
        atomic { implicit txn =>
          removeOpenConnection(session, connection)
          connection.stream() = null
          val heartBeatTimer = stream.heartBeatTimer()
          stream.heartBeatTimer() = null
          Txn.afterCommit(_ => heartBeatTimer.cancel(false))
          connection.unconfirmedPack().foldLeft(connection.numAcknowledgeReceivedForData()) {
            case (packId, Data(buffer)) => {
              enqueue(session, RetransmissionData(connectionId, packId, buffer))
              packId + 1
            }
            case (packId, Finish) => {
              RetransmissionFinish(connectionId, packId)
              enqueue(session, RetransmissionFinish(connectionId, packId))
              packId + 1
            }
            case (nextPackId, pack) => {
              enqueue(session, pack)
              nextPackId
            }
          }
          connection.unconfirmedPack() = Queue.empty
          checkConnectionFinish(session, connectionId, connection)
        }
      }
    }
    for (clientToServer <- BcpIo.receive(stream)) {
      clientToServer match {
        case HeartBeat => {
          startReceive(session, connectionId, connection, stream)
        }
        case Data(buffer) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            val packId = connection.numDataReceived()
            connection.numDataReceived() = packId + 1
            dataReceived(session, connectionId, connection, packId, buffer)
          }
          startReceive(session, connectionId, connection, stream)
          stream.flush()
        }
        case RetransmissionData(dataConnectionId, packId, data) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            session.connections.get(dataConnectionId) match {
              case Some(dataConnection) => {
                dataReceived(session, dataConnectionId, dataConnection, packId, data)
              }
              case None => {
                val lastConnectionId = session.lastConnectionId()
                if (between(lastConnectionId, lastConnectionId + MaxConnectionsPerSession, dataConnectionId)) {
                  // 在成功建立连接以前先收到重传的数据，这表示原连接在BCP握手阶段卡住了
                  val newConnection = new Connection
                  session.connections(dataConnectionId) = newConnection
                  dataReceived(session, dataConnectionId, newConnection, packId, data)
                } else {
                  // 原连接先前已经接收过所有数据并关闭了，可以安全忽略数据
                }
              }
            }
          }
          startReceive(session, connectionId, connection, stream)
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
            checkConnectionFinish(session, connectionId, connection)
          }
          startReceive(session, connectionId, connection, stream)
        }
        case Finish => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            val packId = connection.numDataReceived()
            finishReceived(session, connectionId, connection, packId)
          }
          startReceive(session, connectionId, connection, stream)
          stream.flush()
        }
        case RetransmissionFinish(finishConnectionId, packId) => {
          BcpIo.enqueue(stream, Acknowledge)
          atomic { implicit txn =>
            resetHeartBeatTimer(stream)
            session.connections.get(finishConnectionId) match {
              case Some(finishConnection) => {
                finishReceived(session, finishConnectionId, finishConnection, packId)
              }
              case None => {
                val lastConnectionId = session.lastConnectionId()
                if (between(lastConnectionId, lastConnectionId + MaxConnectionsPerSession, connectionId)) {
                  // 在成功建立连接以前先收到重传的数据，这表示原连接在BCP握手阶段卡住了
                  val newConnection = new Connection
                  session.connections(finishConnectionId) = newConnection
                  finishReceived(session, finishConnectionId, newConnection, packId)
                } else {
                  // 原连接先前已经接收过所有数据并关闭了，可以安全忽略数据
                }
              }
            }
          }
          startReceive(session, connectionId, connection, stream)
          stream.flush()
        }
        case ShutDown => {
          atomic { implicit txn =>
            checkShutDown(session)
          }
        }
        case Renew => {
          atomic { implicit txn =>
            session.sendingQueue() match {
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
            session.sendingQueue() = Right(SendingConnectionQueue(Set(connection), Set(connection)))
            session.connections.clear()
            session.connections(connectionId) = connection
          }
        }
      }
    }
  }

  private def resetHeartBeatTimer(stream: Stream)(implicit txn: InTxn) {
    val oldTimer = stream.heartBeatTimer()
    Txn.afterCommit(_ => oldTimer.cancel(false))
    val newTimer =
      executor.scheduleWithFixedDelay(
        stream,
        ServerHeartBeatDelay.length,
        ServerHeartBeatDelay.length,
        ServerHeartBeatDelay.unit)
    Txn.afterRollback(_ => newTimer.cancel(false))
    stream.heartBeatTimer() = newTimer
  }

  final def shutDown(session: Session) {
    atomic { implicit txn: InTxn =>
      checkShutDown(session)
    }
  }

  final def send(session: Session, buffer: ByteBuffer*) {
    atomic { implicit txn =>
      enqueue(session, Data(buffer))
    }
  }

  protected final def addIncomingSocket(socket: AsynchronousSocketChannel) {
    val stream = new Stream(socket)
    implicit def catcher: Catcher[Unit] = PartialFunction.empty
    for (ConnectionHead(sessionId, connectionId) <- BcpIo.receiveHead(stream)) {
      atomic { implicit txn =>
        val session = sessions.get(sessionId) match {
          case None => {
            val session = classTag[Session].runtimeClass.newInstance().asInstanceOf[Session]
            session.sessionId = sessionId
            sessions(sessionId) = session
            Txn.afterCommit(_ => open(session))
            session
          }
          case Some(session) => {
            if (session.connections.size >= MaxConnectionsPerSession) {
              stream.interrupt()
            }
            session
          }
        }
        val connection = session.connections.getOrElseUpdate(connectionId, new Connection)
        session.lastConnectionId() = connectionId
        if (connection.stream() == null) {
          connection.stream() = stream
          addOpenConnection(session, connection)
          Txn.afterCommit(_ => startReceive(session, connectionId, connection, stream))
          val timer =
            executor.scheduleWithFixedDelay(
              stream,
              ServerHeartBeatDelay.length,
              ServerHeartBeatDelay.length,
              ServerHeartBeatDelay.unit)
          Txn.afterRollback(_ => timer.cancel(false))
          stream.heartBeatTimer() = timer
        } else {
          logger.fine(fast"A client atempted to reuse existed connectionId. I rejected it.")
          stream.interrupt()
        }
      }
    }

  }

}
