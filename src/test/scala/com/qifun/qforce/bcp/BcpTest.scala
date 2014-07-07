package com.qifun.qforce.bcp

import org.junit.Test
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
import com.qifun.statelessFuture.util.Zip
import com.qifun.statelessFuture.util.Promise
import java.nio.channels.CompletionHandler
import scala.util.Try
import scala.reflect.ClassTag
import scala.util.Success
import scala.util.Failure

class BcpTest {

  abstract class TestServer extends BcpServer {

    val executor = Executors.newScheduledThreadPool(1)

    val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)

    val serverSocket = AsynchronousServerSocketChannel.open(channelGroup)

    protected def acceptFailed(throwable: Throwable): Unit

    private def startAccept(serverSocket: AsynchronousServerSocketChannel) {
      serverSocket.accept(this, new CompletionHandler[AsynchronousSocketChannel, TestServer] {
        def completed(newSocket: java.nio.channels.AsynchronousSocketChannel, server: TestServer): Unit = {
          addIncomingSocket(newSocket)
          startAccept(serverSocket)
        }
        def failed(throwable: Throwable, server: TestServer): Unit = {
          acceptFailed(throwable)
        }
      })
    }

    final def clear() {
      serverSocket.close()
    }

    serverSocket.bind(null)
    startAccept(serverSocket)
  }

  @Test
  def pingPong(): Unit = {
    val lock = new AnyRef
    @volatile var serverResult: Option[Try[String]] = None
    @volatile var clientResult: Option[Try[String]] = None

    abstract class ServerSession { _: BcpServer#Session =>

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

      override final def shutedDown(): Unit = {}

      override final def unavailable(): Unit = {}

    }

    val server = new TestServer {
      override protected final def newSession(id: Array[Byte]) = new ServerSession with Session {
        override protected final val sessionId = id
      }

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
        val socket = AsynchronousSocketChannel.open()
        Nio2Future.connect(socket, new InetSocketAddress("localhost", server.serverSocket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)).await
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

    server.clear()
  }

}