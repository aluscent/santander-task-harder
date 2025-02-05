package com.alitariverdy
package utils

import data.rows.Guarantor

import scala.util.parsing.combinator._

object GuarantorParser extends RegexParsers {
  // Basic tokens
  def stringLiteral: Parser[String] = "\"" ~> """[^"]*""".r <~ "\""
  def number: Parser[Double] = """-?\d+(\.\d+)?""".r ^^ { _.toDouble }
  def ws: Parser[String] = """\s*""".r

  // JSON structure
  def obj: Parser[Guarantor] =
    "{" ~> ws ~>
      "\"NAME\"" ~> ws ~> ":" ~> ws ~> stringLiteral ~
      (ws ~> "," ~> ws ~> "\"PERCENTAGE\"" ~> ws ~> ":" ~> ws ~> number) <~
      ws <~ "}" ^^ {
      case name ~ percentage => Guarantor(name, percentage)
    }

  def array: Parser[List[Guarantor]] =
    "[" ~> ws ~> repsep(obj, ws ~> "," <~ ws) <~ ws <~ "]"

  def parseJson(input: String): Either[String, Seq[Guarantor]] =
    parseAll(array, input) match {
      case Success(result, _) => Right(result)
      case Failure(msg, _)    => Left(s"Parse failure: $msg")
      case Error(msg, _)      => Left(s"Parse error: $msg")
    }
}
