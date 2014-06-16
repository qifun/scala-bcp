package com.qifun.qforce.bcp.server

sealed abstract class BcpException(message: String = null, cause: Throwable = null)
  extends Exception(message, cause) {
}

object BcpException {
  case object UnknownHeadByte extends BcpException
  case object Eof extends BcpException
  case object VarintTooBig extends BcpException("The varint is too big to read!")
}