package com.qifun.qforce.bcp

import java.util.concurrent.ScheduledExecutorService
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.Executor
import scala.concurrent.stm.InTxn
import com.qifun.statelessFuture.Future
import com.qifun.qforce.bcp.Bcp._
import com.qifun.qforce.bcp.BcpSession._
import scala.util.control.Exception.Catcher
import scala.PartialFunction
import scala.reflect.classTag
import scala.concurrent.stm.atomic
import scala.concurrent.stm.Ref
import java.security.SecureRandom
import com.dongxiguo.fastring.Fastring.Implicits._
import scala.concurrent.stm.Txn

object BcpClient {

  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  private[BcpClient] final class Stream(socket: AsynchronousSocketChannel) extends BcpSession.Stream(socket) {
    // TODO: 客户端专有的数据结构，比如Timer
    val xxx: Int = 1
  }

  private[BcpClient] final class Connection extends BcpSession.Connection[Stream] {

    override private[bcp] final def busy()(implicit txn: InTxn): Unit = {
      atomic { implicit txn =>
        this.stream().xxx
      }
      ??? // TODO: 设置timer，建立新连接 
    }

    override private[bcp] final def idle()(implicit txn: InTxn): Unit = {
      ??? // TODO: 设置timer，关闭多余的连接
    }

  }

}

abstract class BcpClient extends BcpSession[BcpClient.Stream, BcpClient.Connection] {

  import BcpClient.{ logger, formatter, appender }

  override private[bcp] final def newConnection = new BcpClient.Connection

  protected def connect(): Future[AsynchronousSocketChannel]

  protected def executor: ScheduledExecutorService

  override private[bcp] final def internalExecutor: ScheduledExecutorService = executor

  override private[bcp] final def release()(implicit txn: InTxn) {}

  private val sessionId: Array[Byte] = Array.ofDim[Byte](NumBytesSessionId)
  private val nextConnectionId = Ref(0)

  private def start() {
    val secureRandom = new SecureRandom
    secureRandom.setSeed(secureRandom.generateSeed(20))
    secureRandom.nextBytes(sessionId)
    implicit def catcher: Catcher[Unit] = PartialFunction.empty
    for (socket <- connect()) {
      logger.fine(fast"bcp client connect server success, socket: ${socket}")
      val stream = new BcpClient.Stream(socket)
      atomic { implicit txn =>
        val connectionId = nextConnectionId()
        nextConnectionId() = connectionId + 1
        Txn.afterCommit(_ => {
          BcpIo.enqueueHead(stream, ConnectionHead(sessionId, connectionId))
          stream.flush()
          logger.fine(fast"bcp client send head to server success, sessionId: ${sessionId.toSeq} , connectionId: ${connectionId}")
        })
        addStream(connectionId, stream)
      }
    }
  }

  start()

}