package com.qifun.qforce.bcp.client

import java.util.concurrent.ScheduledExecutorService
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import com.qifun.qforce.bcp.BcpSession
import java.util.concurrent.Executor
import scala.concurrent.stm.InTxn

abstract class BcpClient extends BcpSession {

  protected def connect(): AsynchronousSocketChannel

  protected def executor: ScheduledExecutorService

  override private[bcp] def internalExecutor: ScheduledExecutorService = executor

  override final private[bcp] def release()(implicit txn: InTxn) {}

}