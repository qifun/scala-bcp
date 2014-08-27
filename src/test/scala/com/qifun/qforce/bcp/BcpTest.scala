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

package com.qifun.qforce.bcp

import org.junit._
import Assert._
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.nio.ByteBuffer
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.util.io.Nio2Future
import java.net.InetSocketAddress
import org.junit.Assert._
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.AsynchronousServerSocketChannel
import com.qifun.statelessFuture.util.Blocking
import java.util.concurrent.Executors
import scala.reflect.classTag
import com.qifun.statelessFuture.util.Promise
import java.nio.channels.CompletionHandler
import scala.util.Try
import scala.reflect.ClassTag
import scala.util.Success
import scala.util.Failure
import java.nio.channels.ShutdownChannelGroupException
import java.io.IOException
import scala.concurrent.stm.Ref
import scala.concurrent.stm._
import java.util.concurrent.TimeUnit

object BcpTest {
  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)
}

class BcpTest {
  import BcpTest.{ logger, formatter, appender }

  abstract class TestServer extends BcpServer {

    val executor = Executors.newScheduledThreadPool(1)

    val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)

    val serverSocket = AsynchronousServerSocketChannel.open(channelGroup)

    protected def acceptFailed(throwable: Throwable): Unit

    private def startAccept(serverSocket: AsynchronousServerSocketChannel) {
      try {
        serverSocket.accept(this, new CompletionHandler[AsynchronousSocketChannel, TestServer] {
          def completed(newSocket: java.nio.channels.AsynchronousSocketChannel, server: TestServer): Unit = {
            addIncomingSocket(newSocket)
            startAccept(serverSocket)
          }
          def failed(throwable: Throwable, server: TestServer): Unit = {
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

    serverSocket.bind(null)
    startAccept(serverSocket)
  }

  @Test
  def pingPong(): Unit = {
    val lock = new AnyRef
    @volatile var serverResult: Option[Try[String]] = None
    @volatile var clientResult: Option[Try[String]] = None

    trait ServerSession { _: BcpServer#Session =>
      
      override final def available(): Unit = {}

      override final def accepted(): Unit = {}

      override final def received(pack: ByteBuffer*): Unit = {
        lock.synchronized {
          val bytes: Array[Byte] = new Array[Byte](pack.head.remaining())
          pack.head.get(bytes)
          serverResult = Some(Success(new String(bytes, "UTF-8")))
          send(ByteBuffer.wrap("pong".getBytes("UTF-8")))
          lock.notify()
        }
      }

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {
        lock.synchronized {
          serverResult = Some(Failure(throwable))
          lock.notify()
        }
      }
    }

    val client = new BcpClient {

      override final def available(): Unit = {}

      override final def connect(): Future[AsynchronousSocketChannel] = Future[AsynchronousSocketChannel] {
        val socket = AsynchronousSocketChannel.open(server.channelGroup)
        Nio2Future.connect(
          socket,
          new InetSocketAddress(
            "localhost",
            server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
        socket
      }

      override final def executor = new ScheduledThreadPoolExecutor(2)

      override final def received(pack: ByteBuffer*): Unit = {
        lock.synchronized {
          val bytes: Array[Byte] = new Array[Byte](pack.head.remaining())
          pack.head.get(bytes)
          clientResult = Some(Success(new String(bytes, "UTF-8")))
          lock.notify()
        }
      }

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    client.send(ByteBuffer.wrap("ping".getBytes("UTF-8")))

    lock.synchronized {
      while (serverResult == None || clientResult == None) {
        lock.wait()
      }
    }

    val Some(serverSome) = serverResult
    val Some(clientSome) = clientResult

    serverSome match {
      case Success(u) => assertEquals(u, "ping")
      case Failure(e) => throw e
    }

    clientSome match {
      case Success(u) => assertEquals(u, "pong")
      case Failure(e) => throw e
    }

    client.shutDown()
    server.clear()
  }

  @Test
  def shutDownTest {
    val lock = new AnyRef
    @volatile var shutedDownResult: Option[Try[Boolean]] = None

    trait ServerSession { _: BcpServer#Session =>

      override final def available(): Unit = {}

      override final def accepted(): Unit = {}

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {
        lock.synchronized {
          shutedDownResult = Some(Failure(throwable))
          lock.notify()
        }
      }
    }

    val client = new BcpClient {

      override final def available(): Unit = {
      }

      override final def connect(): Future[AsynchronousSocketChannel] = Future[AsynchronousSocketChannel] {
        val socket = AsynchronousSocketChannel.open(server.channelGroup)
        Nio2Future.connect(
          socket,
          new InetSocketAddress(
            "localhost",
            server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
        socket
      }

      override final def executor = new ScheduledThreadPoolExecutor(2)

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {
        lock.synchronized {
          shutedDownResult = Some(Success(true))
          lock.notify()
        }
      }

      override final def unavailable(): Unit = {}

    }

    client.shutDown()

    lock.synchronized {
      while (shutedDownResult == None) {
        lock.wait()
      }
    }

    val Some(clientShutedDownSome) = shutedDownResult

    clientShutedDownSome match {
      case Success(u) => assertEquals(u, true)
      case Failure(e) => throw e
    }

    server.clear()
  }

  @Test
  def closeConnectionTest {
    val lock = new AnyRef
    @volatile var serverReceivedResult: Option[Try[String]] = None
    var clientSocket: Option[AsynchronousSocketChannel] = None

    trait ServerSession { _: BcpServer#Session =>

      override final def available(): Unit = {}

      override final def accepted(): Unit = {}

      override final def received(pack: ByteBuffer*): Unit = {
        lock.synchronized {
          val bytes: Array[Byte] = new Array[Byte](pack.head.remaining())
          pack.head.get(bytes)
          serverReceivedResult = Some(Success(new String(bytes, "UTF-8")))
          send(ByteBuffer.wrap("pong".getBytes("UTF-8")))
          lock.notify()
        }
      }

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {
        lock.synchronized {
          serverReceivedResult = Some(Failure(throwable))
          lock.notify()
        }
      }
    }

    val client = new BcpClient {

      override final def available(): Unit = {
        lock.synchronized {
          lock.notify()
        }
      }

      override final def connect(): Future[AsynchronousSocketChannel] = Future[AsynchronousSocketChannel] {
        val socket = AsynchronousSocketChannel.open(server.channelGroup)
        Nio2Future.connect(
          socket,
          new InetSocketAddress(
            "localhost",
            server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
        clientSocket = Some(socket)
        socket
      }

      override final def executor = new ScheduledThreadPoolExecutor(2)

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {
      }

    }

    // 等待连接成功
    lock.synchronized {
      while (clientSocket == None) {
        lock.wait()
      }
    }

    val Some(socket1) = clientSocket

    socket1.close()
    client.send(ByteBuffer.wrap("Hello bcp-server!".getBytes("UTF-8")))
    lock.synchronized {
      while (serverReceivedResult == None) {
        lock.wait()
      }
    }
    val Some(serverReceivedSome) = serverReceivedResult
    serverReceivedSome match {
      case Success(u) => assertEquals(u, "Hello bcp-server!")
      case Failure(e) => throw e
    }

    client.shutDown()
    server.clear()
  }

  @Test
  def seqSendTest {
    val lock = new AnyRef
    @volatile var serverReceivedResults: Option[Try[Seq[String]]] = None
    var clientSocket: Option[AsynchronousSocketChannel] = None

    trait ServerSession { _: BcpServer#Session =>

      override final def available(): Unit = {}

      override final def accepted(): Unit = {}

      override final def received(pack: ByteBuffer*): Unit = {
        lock.synchronized {
          val bytes: Array[Byte] = new Array[Byte](pack.head.remaining())
          pack.head.get(bytes)
          serverReceivedResults match {
            case None =>
              serverReceivedResults = Some(Success(Seq(new String(bytes, "UTF-8"))))
            case Some(Success(receivedResults)) =>
              serverReceivedResults = Some(Success(receivedResults ++ Seq((new String(bytes, "UTF-8")))))
            case Some(Failure(_)) =>
          }
          lock.notify()
        }
      }

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {
        lock.synchronized {
          serverReceivedResults = Some(Failure(throwable))
          lock.notify()
        }
      }
    }

    val client = new BcpClient {

      override final def available(): Unit = {
        lock.synchronized {
          lock.notify()
        }
      }

      override final def connect(): Future[AsynchronousSocketChannel] = Future[AsynchronousSocketChannel] {
        val socket = AsynchronousSocketChannel.open(server.channelGroup)
        Nio2Future.connect(
          socket,
          new InetSocketAddress(
            "localhost",
            server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
        clientSocket = Some(socket)
        socket
      }

      override final def executor = new ScheduledThreadPoolExecutor(2)

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {
      }

    }

    // 等待连接成功
    lock.synchronized {
      while (clientSocket == None) {
        lock.wait()
      }
    }

    val Some(socket1) = clientSocket

    client.send(ByteBuffer.wrap("a".getBytes("UTF-8")))
    client.send(ByteBuffer.wrap("b".getBytes("UTF-8")))
    lock.synchronized {
      while (serverReceivedResults == None ||
        (serverReceivedResults.get.isSuccess && serverReceivedResults.get.get.count(_ => true) < 2)) {
        lock.wait()
      }
    }
    socket1.close()
    if (serverReceivedResults.get.isSuccess) {
      client.send(ByteBuffer.wrap("c".getBytes("UTF-8")))
      client.send(ByteBuffer.wrap("d".getBytes("UTF-8")))
    }
    lock.synchronized {
      while (serverReceivedResults.get.isSuccess && serverReceivedResults.get.get.count(_ => true) < 4) {
        lock.wait()
      }
    }
    val Some(serverReceivedSome) = serverReceivedResults
    serverReceivedSome match {
      case Success(u) => assertEquals(u, Seq("a", "b", "c", "d"))
      case Failure(e) => throw e
    }

    client.shutDown()
    server.clear()
  }

  @Test
  def clientInterrupteTest {
    val lock = new AnyRef
    @volatile var clientInterrupteResult: Option[Try[Boolean]] = None

    trait ServerSession { _: BcpServer#Session =>

      override final def available(): Unit = {}

      override final def accepted(): Unit = {}

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {}

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new Session(id) with ServerSession

      override protected final def acceptFailed(throwable: Throwable): Unit = {
        lock.synchronized {
          clientInterrupteResult = Some(Failure(throwable))
          lock.notify()
        }
      }
    }

    val client = new BcpClient {

      override final def available(): Unit = {}

      override final def connect(): Future[AsynchronousSocketChannel] = Future[AsynchronousSocketChannel] {
        val socket = AsynchronousSocketChannel.open(server.channelGroup)
        Nio2Future.connect(
          socket,
          new InetSocketAddress(
            "localhost",
            server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
        socket.shutdownInput()
        socket
      }

      override final def executor = new ScheduledThreadPoolExecutor(2)

      override final def received(pack: ByteBuffer*): Unit = {}

      override final def interrupted(): Unit = {
        lock.synchronized {
          clientInterrupteResult = Some(Success(true))
          lock.notify()
        }
      }

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {
      }

    }

    lock.synchronized {
      while (clientInterrupteResult == None) {
        lock.wait()
      }
    }

    val Some(clientInterruptedSome) = clientInterrupteResult
    clientInterruptedSome match {
      case Success(u) => assertEquals(u, true)
      case Failure(e) => throw e
    }
    
    client.shutDown()
    server.clear()
  }

}