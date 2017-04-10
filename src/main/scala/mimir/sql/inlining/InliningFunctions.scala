package mimir.sql.inlining

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import mimir.algebra.PrimitiveValue
import mimir.Database
import mimir.algebra.Type

object InliningFunctions {
  
  def interpretInliningFunction(function: String) : universe.Function = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
        val tb = ToolBox(mirror).mkToolBox()
        val functionWrapper = "object FunctionWrapper { " + function + "}"
        val functionSymbol = tb.parse(function).asInstanceOf[tb.u.Function]
        val tree = tb.typeCheck(functionSymbol)
        functionSymbol
  }
  
  def bestGuessVGTerm(db:Database, value_mimir : (Int,Type) => PrimitiveValue) : (String, Int) => PrimitiveValue = {
    val func : (String, Int) => PrimitiveValue = (modelName, idx) => {
      val model = db.models.get(modelName)
      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+2, arg._1) )
      val hintList = 
        model.hintTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+argList.length+2, arg._1) )
      val guess = model.bestGuess(idx, argList, hintList)
      //logger.trace(s"$modelName;$idx: $argList -> $guess")
      guess
    }
    func
  }
}

 
 