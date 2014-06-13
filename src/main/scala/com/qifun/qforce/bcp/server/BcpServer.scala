package com.qifun.qforce.bcp.server

import java.net.InetAddress
import java.nio.channels.AsynchronousSocketChannel
import com.qifun.statelessFuture.Future
import com.qifun.statelessFuture.io.Nio2Future
import com.qifun.statelessFuture.io.SocketWritingQueue
import scala.concurrent.duration._
import com.qifun.statelessFuture.io.SocketInputStream
import java.io.EOFException
import java.util.concurrent.ScheduledExecutorService
import java.nio.channels.AsynchronousChannelGroup

private object BcpServer {

  final class Stream(override protected final val socket: AsynchronousSocketChannel)
    extends SocketInputStream with SocketWritingQueue {

    override protected final def readingTimeout = Bcp.ServerReadingTimeout

    override protected final def writingTimeout = Bcp.ServerWritingTimeout

  }

}

/**
 * BCP协议的特性：
 * 1. 基于连接
 * 2. 可靠，低延时
 * 3. 以数据包为单位，没有流
 * 4. 乱序数据包，不保证接收顺序与发送顺序一致
 */
abstract class BcpServer {

  protected def executor: ScheduledExecutorService
  
  /**
   * 多长时间收不到心跳包就杀掉TCP连接。
   */
  protected def readingTimeout = 5.seconds

  /**
   * 多长时间发不出数据就杀掉TCP连接。
   *
   * 发不出数据只可能因为TCP缓冲区满，
   * 只有发送超大数据包时，客户端很慢，才会发生这种情况，可能性微乎其微。
   */
  protected def writingTimeout = 1.seconds

  final class BcpSession {

  }

  protected final def addIncomingSocket(socket: AsynchronousSocketChannel) {
    val stream = new BcpServer.Stream(socket)
    Future {
      var sessionId = Bcp.readSessionId(stream)
      Bcp.readBcp(stream).await match {
        case Bcp.RenewRequest =>
      }

      //      var readVarint(stream).await
      // stream.available_=(bytesRequired)
    }
  }

  //  
  //  private val listeningSocket = MutableSet.empty
  //  
  //  val listeningAddresses = new MutableSet[InetAddress] {
  //    
  //  }

}
