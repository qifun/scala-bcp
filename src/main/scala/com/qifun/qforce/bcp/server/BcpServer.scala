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
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.util.control.Exception.Catcher
import scala.concurrent.stm.atomic
import scala.concurrent.stm.Ref
import scala.concurrent.stm.TSet
import scala.concurrent.stm.TMap
import java.util.concurrent.ScheduledFuture
import scala.collection.mutable.WrappedArray
import scala.reflect.ClassTag
import scala.reflect.classTag
import java.nio.ByteBuffer
import java.io.InputStream
import scala.util.control.TailCalls

private object BcpServer {

  class IdSetIsFullException extends Exception

  private object RangedIdSet {

    private def between(low: Int, high: Int, test: Int): Boolean = {
      if (low < high) {
        test >= low && test < high
      } else if (low > high) {
        test >= low || test < high
      } else {
        false
      }
    }

    @tailrec
    private def compat(lowId: Int, highId: Int, ids: Set[Int]): RangedIdSet = {
      if (ids(lowId)) {
        compat(lowId + 1, highId, ids - lowId)
      } else {
        new RangedIdSet(lowId, highId, ids)
      }
    }
  }

  final class RangedIdSet(lowId: Int, highId: Int, ids: Set[Int]) extends IdSet {
    import RangedIdSet._

    @throws(classOf[IdSetIsFullException])
    override final def +(id: Int) = {
      if (between(lowId, highId, id)) {
        compat(lowId, highId, ids + id)
      } else if (between(highId, highId + 1024, id)) {
        if (between(lowId, lowId + 2048, id)) {
          throw new IdSetIsFullException
        } else {
          new RangedIdSet(lowId, id, ids + id)
        }
      } else {
        this
      }
    }

    override final def contains(id: Int) = {
      if (between(lowId, highId, id)) {
        ids.contains(id)
      } else if (between(highId, highId + 1024, id)) {
        false
      } else {
        true
      }
    }
  }

  object EmptyIdSet extends IdSet {

    @throws(classOf[IdSetIsFullException])
    override final def +(id: Int) = new RangedIdSet(id + 1, id + 1, Set.empty[Int])

    override final def contains(id: Int) = false

  }

  sealed abstract class IdSet {
    def +(id: Int): IdSet
    def contains(id: Int): Boolean
  }

  final class Stream(override protected final val socket: AsynchronousSocketChannel)
    extends SocketInputStream with SocketWritingQueue {

    override protected final def readingTimeout = Bcp.ServerReadingTimeout

    override protected final def writingTimeout = Bcp.ServerWritingTimeout

  }

}

/**
 * BCP协议的特性：
 *
 * <ol>
 *   <li>基于连接</li>
 *   <li>可靠，低延时</li>
 *   <li>以数据包为单位，没有流</li>
 *   <li>乱序数据包，不保证接收顺序与发送顺序一致</li>
 * </ol>
 */
abstract class BcpServer[Attachment: ClassTag] {

  import BcpServer._

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

  private type HeartBeatDelay = ScheduledFuture[_]
  private type BoxedSessionId = WrappedArray[Byte]

  final class BcpSession {

    private[BcpServer] val receiveIdSet = Ref[IdSet](EmptyIdSet)

    val attachment = classTag[Attachment].runtimeClass.newInstance.asInstanceOf[Attachment]

    private[BcpServer] val receivingStreams = TSet.empty[Stream]

    private val sendingStreams = TMap.empty[Stream, HeartBeatDelay]

  }

  final def send(session: BcpSession, pack: ByteBuffer*) = ???

  protected def open(session: BcpSession)

  protected def closed(session: BcpSession)

  protected def received(session: BcpSession, data: InputStream)

  //  protected def newAttachment(session: BcpSession): Attachment

  private val sessions = TMap.empty[BoxedSessionId, BcpSession]

  private def receive(session: BcpSession, stream: Stream): Future[Nothing] = Future {
    Bcp.receive(stream).await match {
      case Bcp.Data(packId, data) => {
        atomic { implicit txn =>
          val idSet = session.receiveIdSet()
          if (idSet.contains(packId)) {
            // 已经收过了，直接忽略。
          } else {
            session.receiveIdSet() = idSet + packId
            received(session, data)
          }
        }
        receive(session, stream).await
      }
      case Bcp.RenewRequest => ???
      case Bcp.EndData => ???
      case Bcp.EndDataAndWait => ???
    }

    throw new IllegalStateException("Unreachable code!")
  }

  protected final def addIncomingSocket(socket: AsynchronousSocketChannel) {
    val stream = new Stream(socket)
    val sessionFuture = Future {
      var sessionId = Bcp.readSessionId(stream).await
      val (session, isNewSession) =
        atomic { implicit txn =>
          sessions.get(sessionId) match {
            case None => {
              val session = new BcpSession
              sessions(sessionId) = session
              session.receivingStreams += stream
              (session, true)
            }
            case Some(session) => {
              session.receivingStreams += stream
              (session, false)
            }
          }
        }
      if (isNewSession) {
        open(session)
      }
      receive(session, stream).await
    }
    implicit def catcher: Catcher[Unit] = PartialFunction.empty
    for (result <- sessionFuture) {
      // Do nothing
    }
  }

  //  
  //  private val listeningSocket = MutableSet.empty
  //  
  //  val listeningAddresses = new MutableSet[InetAddress] {
  //    
  //  }

}
