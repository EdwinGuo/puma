package parserUtils

import java.io.IOException
import java.lang.Integer.parseInt
import java.lang.Long.parseLong
import com.opencsv.{CSVParser, CSVParserBuilder}
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.reflect.macros._
import scala.language.experimental.macros

object Utils{
  def invoke(className: Option[String], method: String, sparkParams: Option[SparkParams]) = {
    val packageName = "Flow.SparkJob."
    val sufix = "Job"
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(Class.forName(packageName + className.get + sufix))

    val targetMethod = moduleSymbol.typeSignature
      .members
      .filter(x => x.isMethod && x.name.toString == method)
      .head
      .asMethod

    runtimeMirror.reflect(runtimeMirror.reflectModule(moduleSymbol).instance)
      .reflectMethod(targetMethod)(sparkParams.get)
  }

  implicit def toInt(w: String): Int = parseInt(w)

  implicit def toLong(w: String): Long = parseLong(w)

  implicit def toString(w: String): String = w

  def convertField[A](field: String)(implicit f: String => A): Option[A] = {
    if (field == "-" | field.isEmpty) {
      None
    } else {
      Some(f(field))
    }
  }

  def nameOf(expr: Any): String = macro getName

  def getName(c: Context)(expr: c.Expr[Any]): c.Expr[String] = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree): c.Name = tree match {
      case Ident(n) => n
      case Select(_, n) => n
      case Function(_, body) => extract(body)
      case Block(_, expr) => extract(expr)
      case Apply(func, _) => extract(func)
      case TypeApply(func, _) => extract(func)
      case _ => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    val name = extract(expr.tree).decoded
    reify {
      c.Expr[String] { Literal(Constant(name)) }.splice
    }
  }

  val logger = LoggerFactory.getLogger(this.getClass)

  def parseCSVString(words: Iterator[String], csv: CSVParser): Iterator[Vector[String]] = {
    val wList = ListBuffer[Vector[String]]()
    while (words.hasNext) {

      val doubleQ = replace(words.next, "\"\"", "''")
      val next = replace(doubleQ, "\\\"", "\"")

      try {
        val p = csv.parseLine(next)
        wList.append(p.toVector)
      }
      catch {
        case e: IOException => {
          logger.error(next);
          e.printStackTrace();
          e.toString
        }
      }
    }
    return wList.toIterator
  }

  def replace(source: String, os: String, ns: String): String = {
    if (source == null) {
      null
    }
    var _source = source
    var i = 0
    if ({i = _source.indexOf(os, i); i >= 0}) {
      val sourceArray = _source.toCharArray
      val nsArray = ns.toCharArray
      val oLength = os.length
      val buf = new java.lang.StringBuilder(sourceArray.length)
      buf.append(sourceArray, 0, i).append(nsArray)
      i += oLength
      var j = i
      while ({i = _source.indexOf(os, i); i > 0}) {
        buf.append(sourceArray, j, i - j).append(nsArray)
        i += oLength
        j = i
      }
      buf.append(sourceArray, j, sourceArray.length - j)
      _source = buf.toString
      buf.setLength(0)
    }
    return _source
  }

}
