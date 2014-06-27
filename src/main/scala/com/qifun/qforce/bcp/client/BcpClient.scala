package com.qifun.qforce.bcp.client

import java.util.concurrent.ScheduledExecutorService
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import com.qifun.qforce.bcp.BcpSession
import java.util.concurrent.Executor
import scala.concurrent.stm.InTxn
import com.qifun.statelessFuture.Future
import com.qifun.qforce.bcp.Bcp._
import scala.util.Random
import com.qifun.qforce.bcp.BcpSession
import com.qifun.qforce.bcp.BcpSession._
import scala.util.control.Exception.Catcher
import scala.PartialFunction
import com.qifun.qforce.bcp.BcpIo
import scala.reflect.classTag
import scala.concurrent.stm.atomic
import scala.concurrent.stm.Ref

abstract class BcpClient extends BcpSession {

  protected def connect(): Future[AsynchronousSocketChannel]

  protected def executor: ScheduledExecutorService

  override private[bcp] def internalExecutor: ScheduledExecutorService = executor

  override final private[bcp] def release()(implicit txn: InTxn) {}

  private val sessionId: Array[Byte] = Array[Byte](NumBytesSessionId)
  private val connectionId = Ref(0)

  private def start() {
    Random.nextBytes(sessionId)
    implicit def catcher: Catcher[Unit] = PartialFunction.empty
    for (socket <- connect()) {
      val stream = new Stream(socket)
      atomic { implicit txn =>
        BcpIo.enqueueHead(stream, sessionId, connectionId())
        addStream(connectionId(), stream)
      }
    }
  }

  start()
}