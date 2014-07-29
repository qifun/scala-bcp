package com.qifun.qforce.bcp

import java.nio.channels.AsynchronousSocketChannel
import java.security.SecureRandom
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import scala.PartialFunction
import scala.concurrent.duration._
import scala.concurrent.stm.InTxn
import scala.concurrent.stm.Ref
import scala.concurrent.stm.Txn
import scala.concurrent.stm.atomic
import scala.reflect.classTag
import scala.util.control.Exception.Catcher
import com.dongxiguo.fastring.Fastring.Implicits._
import com.qifun.qforce.bcp.Bcp._
import com.qifun.qforce.bcp.BcpSession._
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.util.Blocking

object BcpClient {

  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  private[BcpClient] final class Stream(socket: AsynchronousSocketChannel) extends BcpSession.Stream(socket) {
    // 客户端专有的数据结构，比如Timer
    val busyTimer = Ref.make[ScheduledFuture[_]]
    val connectionState: Ref[ConnectionState] = Ref(ConnectionIdle)
  }

  private[BcpClient] final class Connection extends BcpSession.Connection[Stream] {
  }

}

/**
 * BCP协议客户端
 *
 * 每个BCP客户端管理多个TCP连接，最多不超过[[Bcp.MaxConnectionsPerSession]]。
 *
 * BCP客户端根据网络状况决定要增加TCP连接还是减少TCP连接。
 *
 * 对于每个TCP连接，有空闲、繁忙和迟缓三种状态：
 *
 *  - 如果BCP客户端从某个TCP连接发出的所有[[Bcp.AcknowledgeRequired]]，都收到了对应的[[Bcp.Acknowledge]]，
 *    那么这个TCP连接是空闲状态。
 *  - 如果某个TCP连接，原本是空闲状态，接着发送了一个[[Bcp.AcknowledgeRequired]]，
 *    那么这个TCP连接变成繁忙状态。
 *  - 如果某个TCP连接的繁忙状态保持了[[Bcp.BusyTimeout]]，还没有变回空闲状态，
 *    那么这个TCP连接变成迟缓状态。
 *
 * 每当一个BCP客户端的所有TCP连接都变成迟缓状态，且BCP客户端管理的TCP连接数尚未达到上限，BCP客户端建立新TCP连接。
 *
 * 如果一个BCP客户端管理的TCP连接数量大于一，且这个BCP客户端所属的空闲TCP连接数量大于零，那么这个BCP客户端是过剩状态。
 *
 * 如果BCP客户端连续[[Bcp.IdleTimeout]]，都在过剩状态，那么这个BCP客户端关掉一个TCP连接。
 *
 */
abstract class BcpClient extends BcpSession[BcpClient.Stream, BcpClient.Connection] {
  
  // TODO 添加一个构造函数，崩溃时重置功能
  
  // TODO 添加一个renew接口，Unavailable太长时间时重置功能

  import BcpClient.{ logger, formatter, appender }

  private val reconnectTimer = Ref.make[ScheduledFuture[_]]
  private val idleTimer = Ref.make[ScheduledFuture[_]]
  private val sessionId: Array[Byte] = Array.ofDim[Byte](NumBytesSessionId)
  private val nextConnectionId = Ref(0)
  private val isConnecting = Ref(false)
  private val isShutedDown = Ref(false)

  override private[bcp] final def newConnection = new BcpClient.Connection

  protected def connect(): Future[AsynchronousSocketChannel]

  protected def executor: ScheduledExecutorService

  override private[bcp] final def internalExecutor: ScheduledExecutorService = executor

  override private[bcp] final def release()(implicit txn: InTxn) {
    isShutedDown() = true
    val oldReconnectTimer = reconnectTimer()
    reconnectTimer() = null;
    if (oldReconnectTimer != null) {
      Txn.afterCommit(_ => oldReconnectTimer.cancel(false))
    }
    val oldIdleTimer = idleTimer()
    idleTimer() = null;
    if (oldIdleTimer != null) {
      Txn.afterCommit(_ => oldIdleTimer.cancel(false))
    }
  }

  override private[bcp] final def busy(busyConnection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    logger.info("the connection is busy!")
    val oldReconnectTimer = reconnectTimer()
    if (oldReconnectTimer != null) {
      reconnectTimer() = null
      Txn.afterCommit(_ => oldReconnectTimer.cancel(false))
    }
    val oldBusyTimer = busyConnection.stream().busyTimer()
    if (oldBusyTimer == null || oldBusyTimer.isDone()) {
      val newBusyTimer = internalExecutor.schedule(new Runnable() {
        def run() {
          try {
            atomic { implicit txn =>
              if (busyConnection.stream() != null) {
                busyConnection.stream().connectionState() = ConnectionSlow
              }
              increaseConnection()
            }
          } catch {
            case e: Exception =>
              logger.severe(e)
          }
        }
      }, BusyTimeout.length, BusyTimeout.unit)
      Txn.afterRollback(_ => newBusyTimer.cancel(false))
      busyConnection.stream().busyTimer() = newBusyTimer
      busyConnection.stream().connectionState() = ConnectionBusy
      // bcp-client 不是过剩状态
      if (!(connections.size > 1 &&
        connections.exists(connection =>
          connection._2.stream() != null && connection._2.stream().connectionState() == ConnectionIdle))) {
        if (idleTimer() != null && !idleTimer().isDone()) {
          Txn.afterCommit(_ => idleTimer().cancel(false))
        }
      }
    }
  }

  override private[bcp] final def idle(connection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    logger.info("the connection is idle!")
    val busyTimer = connection.stream().busyTimer()
    Txn.afterCommit(_ => busyTimer.cancel(false))
    connection.stream().connectionState() = ConnectionIdle
    checkFinishConnection()
  }

  override private[bcp] final def close(closeConnection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    val connectionSize = connections.size
    if (closeConnection.stream() != null) {
      val busyTimer = closeConnection.stream().busyTimer()
      if (busyTimer != null) {
        closeConnection.stream().busyTimer() = null
        Txn.afterCommit(_ => busyTimer.cancel(false))
      }
    }
    if (connections.forall(connection =>
      connection._2 == closeConnection || connection._2.stream() == null) &&
      connectionSize < MaxConnectionsPerSession) {
      startReconnectTimer()
    }
    if (connectionSize >= MaxConnectionsPerSession &&
      connections.forall(connection =>
        connection._2 == closeConnection || connection._2.stream() == null)) {
      internalInterrupt()
    }
  }

  private def afterConnect(socket: AsynchronousSocketChannel, connectionId: Int): Future[Unit] = Future {
    logger.fine(fast"bcp client connect server success, socket: ${socket}")
    val stream = new BcpClient.Stream(socket)
    atomic { implicit txn =>
      if (!isShutedDown()) {
        Txn.afterCommit { _ =>
          BcpIo.enqueueHead(stream, ConnectionHead(sessionId, false, connectionId))
          logger.fine(
            fast"bcp client send head to server success, sessionId: ${sessionId.toSeq} , connectionId: ${connectionId}")
        }
        addStream(connectionId, stream)
        isConnecting() = false
      } else {
        socket.close()
      }
    }
    stream.flush()
  }

  private final def increaseConnection()(implicit txn: InTxn) {
    if (!isConnecting() &&
      connections.size < MaxConnectionsPerSession &&
      connections.count(_._2.stream() != null) < MaxActiveConnectionsPerSession &&
      connections.forall(connection =>
        connection._2.stream() == null || connection._2.stream().connectionState() == ConnectionSlow)) {
      isConnecting() = true
      val connectionId = nextConnectionId() + 1
      nextConnectionId() = nextConnectionId() + 1
      Txn.afterCommit { _ =>
        val connectFuture = Future {
          val socket = connect().await
          afterConnect(socket, connectionId).await
        }
        implicit def catcher: Catcher[Unit] = {
          case e: Exception => {
            logger.severe(e)
            atomic { implicit txn =>
              if (!isShutedDown()) {
                startReconnectTimer()
              }
            }
          }
        }
        for (_ <- connectFuture) {
          logger.fine("Increase connection success.")
        }
      }
    }
  }

  private final def checkFinishConnection()(implicit txn: InTxn) {
    if (connections.size > 1 &&
      connections.exists(connection =>
        connection._2.stream() != null && connection._2.stream().connectionState() == ConnectionIdle)) { // 过剩状态
      if (idleTimer() == null || idleTimer().isDone()) {
        val newIdleTimer = internalExecutor.schedule(new Runnable() {
          def run() {
            try {
              atomic { implicit txn =>
                connections.find(connection =>
                  (connection._2.stream() != null) &&
                    (connection._2.stream().connectionState() == ConnectionIdle)) match {
                  case Some((connectionId, toFinishConnection)) =>
                    finishConnection(connectionId, toFinishConnection)
                  case None =>
                }
              }
            } catch {
              case e: Exception =>
                logger.severe(e)
            }
          }
        }, IdleTimeout.length, IdleTimeout.unit)
      }
    }
  }

  private final def startReconnectTimer()(implicit txn: InTxn): Unit = {
    if (reconnectTimer() == null || reconnectTimer().isDone()) {
      val newBusyTimer = internalExecutor.schedule(new Runnable() {
        def run() {
          try {
            atomic { implicit txn =>
              increaseConnection()
            }
          } catch {
            case e: Exception =>
              logger.severe(e)
          }
        }
      }, ReconnectTimeout.length, ReconnectTimeout.unit)
      Txn.afterRollback(_ => newBusyTimer.cancel(false))
      reconnectTimer() = newBusyTimer
    }
  }

  private final def start() {
    atomic { implicit txn: InTxn =>
      val secureRandom = new SecureRandom
      secureRandom.setSeed(secureRandom.generateSeed(20))
      secureRandom.nextBytes(sessionId)
      increaseConnection()
    }
  }

  start()

}