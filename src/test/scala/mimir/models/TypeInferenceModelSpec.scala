package mimir.models

import org.specs2.mutable._
import mimir.algebra._
import mimir.algebra.Type._

object TypeInferenceModelSpec extends Specification
{

  def train(elems: List[String]): Model = 
  {
    val model = new TypeInferenceModel("TEST_MODEL", "TEST_COLUMN", 0.5)
    elems.foreach( model.learn(_) )
    return model
  }

  def guess(elems: List[String]): Type.T =
  {
    train(elems).
      bestGuess(0, List[PrimitiveValue]()) match {
        case TypePrimitive(t) => t
      }
  }


  "The Type Inference Model" should {

    "Recognize Integers" >> {
      guess(List("1", "2", "3", "500", "29", "50")) must be equalTo(TInt)
    }
    "Recognize Floats" >> {
      guess(List("1.0", "2.0", "3.2", "500.1", "29.9", "50.0000")) must be equalTo(TFloat)
      guess(List("1", "2", "3", "500", "29", "50.0000")) must be equalTo(TFloat)
    }
    "Recognize Dates" >> {
      guess(List("1984-11-05", "1951-03-23", "1815-12-10")) must be equalTo(TDate)
    }
    "Recognize Strings" >> {
      guess(List("Alice", "Bob", "Carol", "Dave")) must be equalTo(TString)
      guess(List("Alice", "Bob", "Carol", "1", "2.0")) must be equalTo(TString)
    }

  }
}