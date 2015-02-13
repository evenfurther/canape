package net.rfc1149.canape

case class StatusCode(code: Int, reason: String) extends RuntimeException(code + " " + reason)

object StatusCode {
  def apply(status: spray.http.StatusCode): StatusCode = StatusCode(status.intValue, status.reason)
}