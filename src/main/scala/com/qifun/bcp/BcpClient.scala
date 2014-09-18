/*
 * scala-bcp
 * Copyright 2014 深圳岂凡网络有限公司 (Shenzhen QiFun Network Corp., LTD)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qifun.bcp

import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.ShutdownChannelGroupException
import java.security.SecureRandom
import java.util.concurrent.CancellationException
import java.util.concurrent.ScheduledExecutorService

import scala.concurrent.stm.InTxn
import scala.concurrent.stm.Ref
import scala.concurrent.stm.TMap.asMap
import scala.concurrent.stm.Txn
import scala.concurrent.stm.atomic
import scala.util.control.Exception.Catcher

import com.dongxiguo.fastring.Fastring.Implicits.FastringContext
import com.dongxiguo.zeroLog.LogRecord.StringLogRecord
import com.dongxiguo.zeroLog.LogRecord.ThrowableLogRecord
import com.qifun.bcp.Bcp.BusyTimeout
import com.qifun.bcp.Bcp.ConnectionBusy
import com.qifun.bcp.Bcp.ConnectionHead
import com.qifun.bcp.Bcp.ConnectionIdle
import com.qifun.bcp.Bcp.ConnectionSlow
import com.qifun.bcp.Bcp.ConnectionState
import com.qifun.bcp.Bcp.IdleTimeout
import com.qifun.bcp.Bcp.MaxActiveConnectionsPerSession
import com.qifun.bcp.Bcp.MaxConnectionsPerSession
import com.qifun.bcp.Bcp.NumBytesSessionId
import com.qifun.bcp.Bcp.ReconnectTimeout
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.Future.apply
import com.qifun.statelessFuture.util.CancellablePromise
import com.qifun.statelessFuture.util.Sleep

object BcpClient {

  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  private[BcpClient] final class Stream(socket: AsynchronousSocketChannel) extends BcpSession.Stream(socket) {
    // 客户端专有的数据结构，比如Timer
    val busyPromise = Ref.make[CancellablePromise[Unit]]
    val connectionState: Ref[ConnectionState] = Ref(ConnectionIdle)
  }

  private[BcpClient] final class Connection extends BcpSession.Connection[Stream] {
  }

  private val sessionIdGenerator = new SecureRandom

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
abstract class BcpClient(sessionId: Array[Byte]) extends BcpSession[BcpClient.Stream, BcpClient.Connection] {

  def this() = this{
    val id = Array.ofDim[Byte](NumBytesSessionId)
    BcpClient.sessionIdGenerator.nextBytes(id)
    id
  }
  // TODO 添加一个构造函数，崩溃时重置功能

  // TODO 添加一个renew接口，Unavailable太长时间时重置功能

  import BcpClient.{ logger, formatter, appender }

  private val reconnectPromise = Ref.make[CancellablePromise[Unit]]
  private val idlePromise = Ref.make[CancellablePromise[Unit]]
  private val nextConnectionId = Ref(0)
  private val isConnecting = Ref(false)
  private val isShutedDown = Ref(false)

  override private[bcp] final def newConnection = new BcpClient.Connection

  protected def connect(): Future[AsynchronousSocketChannel]

  protected def executor: ScheduledExecutorService

  override private[bcp] final def internalExecutor: ScheduledExecutorService = executor

  override private[bcp] final def release()(implicit txn: InTxn) {
    isShutedDown() = true
    val oldReconnectPromise = reconnectPromise()
    reconnectPromise() = null;
    if (oldReconnectPromise != null) {
      Txn.afterCommit(_ => oldReconnectPromise.cancel())
    }
    val oldIdlePromise = idlePromise()
    idlePromise() = null;
    if (oldIdlePromise != null) {
      Txn.afterCommit(_ => oldIdlePromise.cancel())
    }
  }

  private def busyComplete(busyConnection: BcpClient.Connection, thisTimer: CancellablePromise[Unit]): Unit = {
    atomic { implicit txn =>
      if (busyConnection.stream() != null && busyConnection.stream().busyPromise() == thisTimer) {
        busyConnection.stream().connectionState() = ConnectionSlow
        increaseConnection()
        busyConnection.stream().busyPromise() = null
      }
    }
  }

  override private[bcp] final def busy(busyConnection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    logger.finest("the connection is busy!")
    val oldReconnectPromise = reconnectPromise()
    if (oldReconnectPromise != null) {
      reconnectPromise() = null
      Txn.afterCommit(_ => oldReconnectPromise.cancel())
    }
    val oldBusyPromise = busyConnection.stream().busyPromise()
    if (oldBusyPromise == null) {
      implicit def catcher: Catcher[Unit] = {
        case _: CancellationException =>
          logger.finer("The busy timer is cancelled!")
        case e: Exception =>
          logger.warning(e)
      }
      val newBusyPromise = CancellablePromise[Unit]
      busyConnection.stream().busyPromise() = newBusyPromise
      Txn.afterCommit { _ =>
        newBusyPromise.foreach(_ => busyComplete(busyConnection, newBusyPromise))
        Sleep.start(newBusyPromise, executor, BusyTimeout)
      }
      busyConnection.stream().connectionState() = ConnectionBusy
      // bcp-client 不是过剩状态
      if (!(connections.size > 1 &&
        connections.exists(connection =>
          connection._2.stream() != null && connection._2.stream().connectionState() == ConnectionIdle))) {
        val oldIdlePromise = idlePromise()
        if (oldIdlePromise != null) {
          idlePromise() = null
          Txn.afterCommit(_ => oldIdlePromise.cancel())
        }
      }
    }
  }

  override private[bcp] final def idle(connection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    logger.finest("the connection is idle!")
    if (connection.stream() != null) {
      val busyPromise = connection.stream().busyPromise()
      if (busyPromise != null) {
        connection.stream().busyPromise() = null
        Txn.afterCommit(_ => busyPromise.cancel())
      }
      connection.stream().connectionState() = ConnectionIdle
      checkFinishConnection()
    }
  }

  override private[bcp] final def close(closeConnection: BcpClient.Connection)(implicit txn: InTxn): Unit = {
    val connectionSize = connections.size
    if (closeConnection.stream() != null) {
      val busyPromise = closeConnection.stream().busyPromise()
      if (busyPromise != null) {
        closeConnection.stream().busyPromise() = null
        Txn.afterCommit(_ => busyPromise.cancel())
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
    logger.finer(fast"bcp client connect server success, socket: ${socket}")
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
        Txn.afterCommit { _ =>
          socket.close()
        }
      }
    }
    stream.flush()
  }

  private def tryIncreaseConnection(connectionId: Int) {
    val connectFuture = Future {
      val socket = connect().await
      afterConnect(socket, connectionId).await
    }
    implicit def catcher: Catcher[Unit] = {
      case e: ShutdownChannelGroupException => {
        logger.finer(e)
      }
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
      logger.finest("Increase connection success.")
    }
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
        tryIncreaseConnection(connectionId)
      }
    }
  }

  def idleComplete(thisTimer: CancellablePromise[Unit]): Unit = {
    atomic { implicit txn =>
      if (idlePromise() == thisTimer) {
        connections.find(connection =>
          (connection._2.stream() != null) &&
            (connection._2.stream().connectionState() == ConnectionIdle)) match {
          case Some((connectionId, toFinishConnection)) =>
            finishConnection(connectionId, toFinishConnection)
          case None =>
        }
        idlePromise() == null
      }
    }
  }

  private final def checkFinishConnection()(implicit txn: InTxn) {
    if (connections.size > 1 &&
      connections.exists(connection =>
        connection._2.stream() != null && connection._2.stream().connectionState() == ConnectionIdle)) { // 过剩状态
      if (idlePromise() == null) {
        implicit def catcher: Catcher[Unit] = {
          case _: CancellationException =>
            logger.finer("The finish connection is cancelled!")
          case e: Exception =>
            logger.warning(e)
        }
        val newIdlePromise = CancellablePromise[Unit]
        idlePromise() = newIdlePromise
        Txn.afterCommit { _ =>
          newIdlePromise.foreach { _ => idleComplete(newIdlePromise) }
          Sleep.start(newIdlePromise, executor, IdleTimeout)
        }

      }
    }
  }

  private def reconnectComplete(thisTimer: CancellablePromise[Unit]): Unit = {
    atomic { implicit txn =>
      if (reconnectPromise() == thisTimer) {
        reconnectPromise() = null
        increaseConnection()
      }
    }
  }

  private final def startReconnectTimer()(implicit txn: InTxn): Unit = {
    if (reconnectPromise() == null) {
      implicit def catcher: Catcher[Unit] = {
        case _: CancellationException =>
          logger.finer("The reconnect timer is cancelled!")
        case e: Exception =>
          logger.warning(e)
      }
      val newReconnectPromise = CancellablePromise[Unit]
      reconnectPromise() = newReconnectPromise
      Txn.afterCommit { _ =>
        newReconnectPromise.foreach { _ => reconnectComplete(newReconnectPromise) }
        Sleep.start(newReconnectPromise, executor, ReconnectTimeout)
      }
      reconnectPromise() = newReconnectPromise
    }
  }

  final def start() {
    atomic { implicit txn: InTxn =>
      increaseConnection()
    }
  }

}