package net.rfc1149

import net.liftweb.json.JValue

package object canape {

  type mapObject = Map[String, JValue]

  // TODO: remove after transition phase
  @deprecated("use Couch.StatusError instead", "spray") type StatusCode = Couch.StatusError
}
