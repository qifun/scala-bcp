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

import java.nio.ByteBuffer
import org.junit.Test
import scala.concurrent.stm.Ref
import scala.concurrent.stm._
import scala.collection.mutable.WrappedArray
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.ShutdownChannelGroupException
import java.util.concurrent.Executors
import java.nio.channels.CompletionHandler
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.nio.channels.AsynchronousSocketChannel
import java.io.IOException

object ServerDemo {

  object SeqSendServer {
    private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(SeqSendServer.this)
  }

  abstract class SeqSendServer(port: Int) extends BcpServer {

    import SeqSendServer.{ logger, formatter, appender }

    val executor = Executors.newScheduledThreadPool(1)

    val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)

    val serverSocket = AsynchronousServerSocketChannel.open(channelGroup)

    protected def acceptFailed(throwable: Throwable): Unit

    private def startAccept(serverSocket: AsynchronousServerSocketChannel) {
      try {
        serverSocket.accept(SeqSendServer.this, new CompletionHandler[AsynchronousSocketChannel, SeqSendServer] {
          def completed(newSocket: java.nio.channels.AsynchronousSocketChannel, server: SeqSendServer): Unit = {
            addIncomingSocket(newSocket)
            startAccept(serverSocket)
          }
          def failed(throwable: Throwable, server: SeqSendServer): Unit = {
            throwable match {
              case e: IOException =>
                logger.fine(e)
              case _ =>
                acceptFailed(throwable)
            }
          }
        })
      } catch {
        case e: ShutdownChannelGroupException =>
          logger.fine(e)
        case e: Exception =>
          acceptFailed(e)
      }
    }

    final def clear() {
      serverSocket.close()
      channelGroup.shutdown()
      if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
    }

    serverSocket.bind(new InetSocketAddress(port))
    startAccept(serverSocket)
  }

  trait ServerSession { _: BcpServer#Session =>

    final def getTimes() = {}

    override final def available(): Unit = {}

    override final def accepted(): Unit = {}

    override final def received(pack: ByteBuffer*): Unit = {}

    override final def interrupted(): Unit = {}

    override final def shutedDown(): Unit = {}

    override final def unavailable(): Unit = {}

  }

  def main(args: Array[String]): Unit = {

    println("Start server success!");

    val clients = TMap.empty[WrappedArray[Byte], Int]

    val server = new SeqSendServer(3333) {

      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {}

    }

    while (true) {
      atomic { implicit txn: InTxn =>
        for (session <- server.sessions) {
          val sessionId = session._1
          val sendTimes = clients.getOrElseUpdate(sessionId, 0)
          try {
            
            session._2.send(ByteBuffer.wrap(sendTimes.toString.getBytes("UTF-8")))
          } catch {
            case e: Exception =>
          }
          clients.update(sessionId, sendTimes + 1)
        }
      }
      Thread.sleep(1 * 1000)
    }

    server.clear()
  }

}