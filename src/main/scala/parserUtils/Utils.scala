package parserUtils

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
}
