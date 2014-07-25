package com.qifun.qforce.bcp

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

object TestServerDemo {
  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)
}

abstract class TestServerDemo(port: Int) extends BcpServer {

  import TestServerDemo.{ logger, formatter, appender }

  val executor = Executors.newScheduledThreadPool(1)

  val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)

  val serverSocket = AsynchronousServerSocketChannel.open(channelGroup)

  protected def acceptFailed(throwable: Throwable): Unit

  private def startAccept(serverSocket: AsynchronousServerSocketChannel) {
    try {
      serverSocket.accept(this, new CompletionHandler[AsynchronousSocketChannel, TestServerDemo] {
        def completed(newSocket: java.nio.channels.AsynchronousSocketChannel, server: TestServerDemo): Unit = {
          addIncomingSocket(newSocket)
          startAccept(serverSocket)
        }
        def failed(throwable: Throwable, server: TestServerDemo): Unit = {
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

object ServerTestDemo {

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

    val clients = TMap.empty[WrappedArray[Byte], Int]

    val server = new TestServerDemo(3333) {

      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {}

    }

    while (true) {
      atomic { implicit txn: InTxn =>
        for (session <- server.sessions) {
          val sessionId = session._1
          val sendTimes = clients.getOrElseUpdate(sessionId, 0)
          session._2.send(ByteBuffer.wrap(sendTimes.toString.getBytes("UTF-8")))
          clients.update(sessionId, sendTimes + 1)
        }
      }
      Thread.sleep(1 * 1000)
    }

    server.clear()
  }

}