package mimir.lenses

import java.sql.SQLException
import java.util

import mimir.exec.ResultIterator
import mimir.{Analysis, Database}
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables.{CTables, SingleVarModel, VGTerm, Model}
import moa.classifiers.Classifier
import moa.cluster.Cluster
import moa.clusterers.Clusterer
import moa.core.InstancesHeader
import weka.clusterers.{ClusterEvaluation, Cobweb, EM}
import weka.core.matrix.Matrix
import weka.core.{Instance, Attribute, Instances, DenseInstance}

/**
 * Created by vinayak on 11/17/15.
 */
class TemporalLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var model: Model = null
  var timeCol = Eval.evalString(args.head).toUpperCase
  var learnedCols = args.map(Eval.evalString(_).toUpperCase)

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    Project(
      source.schema.map(_._1).zipWithIndex.map {
        case (col, idx) =>
          if (learnedCols.indexOf(col) >= 0 && !col.equals(timeCol))
            ProjectArg(col, VGTerm((name, model), idx, List(Var(CTables.TEMPORAL_VAR))))
          else ProjectArg(col, Var(col))
      },
      source
    )
  }

  override def lensType: String = "TEMPORAL"

  override def schema(): List[(String, T)] = source.schema

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  override def build(db: Database): Unit = {
    model = new TemporalModel(this)
    val idxs = learnedCols.map(a => schema().map(_._1).indexOf(a))
    model.asInstanceOf[TemporalModel].init(db.query(source), idxs)
  }
}

case class TemporalAnalysis(model: TemporalModel, idx: Int, args: List[Expression])
  extends Proc(args) {
  override def get(v: List[PrimitiveValue]): PrimitiveValue = model.mostLikelyValue(idx, v)

  override def rebuild(c: List[Expression]): Expression = TemporalAnalysis(model, idx, c)

  override def exprType(bindings: Map[String, T]): T = Type.TFloat
}

class TemporalModel(lens: TemporalLens) extends Model {
  var clusterer: Cobweb = new Cobweb()
  var transition: Array[Array[Double]] = null
  var emission: Array[Array[Double]] = null
  var observations: List[Double] = null
  var alpha: Array[Double] = null
  var timeIdx: Int = -1
  var colIdx: Int = -1
  var lastTimeStamp: Long = -1
  var timeDiff: Long = -1

  def init(iterator: ResultIterator, idxs:List[Int]): Unit = {
    timeIdx = idxs(0)
    colIdx = idxs(1)
    val attributes = new util.ArrayList[Attribute]()
    iterator.schema.foreach {
      case (c, t) => t match {
        case Type.TInt | Type.TFloat => attributes.add(new Attribute(c))
        case _ => {
          val timecolname = lens.schema().map(_._1).toList(timeIdx)
          c match {
            case timecolname => attributes.add(new Attribute(c))
            case _ => attributes.add(new Attribute(c, null.asInstanceOf[util.ArrayList[String]]))
          }
        }
      }
    }
    val colSet = collection.mutable.HashSet[Double]()
    val data: Instances = new Instances("data", attributes, 100)
    var t0: Long = -1
    //TODO check this
    iterator.getNext()
    while(iterator.getNext()){
      val instance = new DenseInstance(iterator.numCols)
      instance.setDataset(data)
      for(j <- 0 until iterator.numCols){
        iterator.schema(j)._2 match {
          case Type.TInt | Type.TFloat =>
            instance.setValue(j, iterator(j).asDouble)
            if(j == colIdx)
              colSet.add(iterator(j).asDouble)
          case _ =>
            if(j == timeIdx){
              instance.setValue(j, 0)
              lastTimeStamp = util.Date.parse(iterator(j).asString)
              if(timeDiff == -1){
                if(t0 == -1){
                  println("t0 => " + iterator(j).asString)
                  t0 = util.Date.parse(iterator(j).asString)
                } else {
                  println("t1 => " + iterator(j).asString)
                  timeDiff = util.Date.parse(iterator(j).asString) - t0
                }
              }
            }
            else if(!iterator(j).isInstanceOf[NullPrimitive])
              instance.setValue(j, iterator(j).asString)
        }
      }
      clusterer.updateClusterer(instance)
      data.add(instance)
    }
    clusterer.updateFinished()
    val eval = new ClusterEvaluation()
    eval.setClusterer(clusterer)
    eval.evaluateClusterer(data)
    observations = colSet.toList.sorted
    val (t, e) = countStates(data, eval.getNumClusters)
    transition = t
    emission = e

    for(i <- transition.indices){
      val t = transition(i)
      val ts = t.sum
      for(j <- t.indices)
        if(ts != 0)
          t(j) = t(j)/ts
    }
    for(i <- emission.indices){
      val t = emission(i)
      val ts = t.sum
      for(j <- t.indices)
        if(ts != 0)
          t(j) = t(j)/ts
    }
    forwardStep(data, eval.getNumClusters)
    iterator.close()

    println("clusters " + eval.getNumClusters)
    println(colIdx)
    println(lastTimeStamp)
    println(timeDiff)
    println("transition")
    transition.foreach(a => {a.foreach(b => print(b + " ")); println("")})
    println("emission")
    emission.foreach(a => {a.foreach(b => print(b + " ")); println("")})
    print("observations ")
    println(observations)
    print("alpha ")
    alpha.foreach(a => print(a + " "))
  }

  def forwardStep(data: Instances, stateCount: Int): Unit = {
    println("-----------------")
    var dist = new Matrix(Array.fill[Double](1, stateCount)(1.0/stateCount))
    val transitionMat = new Matrix(transition)
    for(i <- 0 until data.size()){
      val o = observations.indexOf(data.get(i).value(colIdx))
      val transitionRow: Array[Array[Double]] = Array[Array[Double]](emission(o))
      val emissionRow = new Matrix(transitionRow)
      //
      println("dist")
      dist.getArray.foreach(a => {a.foreach(b => print(b + " ")); println("")})
      println("emission")
      emissionRow.getArray.foreach(a => {a.foreach(b => print(b + " ")); println("")})
      //
      dist = dist.times(transitionMat).arrayTimes(emissionRow)
    }
    println("-----------------")
    alpha = dist.getArray()(0)
  }

  def countStates(data: Instances, stateCount: Int): (Array[Array[Double]], Array[Array[Double]]) = {
    val obs = Array.ofDim[Double](observations.length, stateCount)
    var prev: Int = 0
    val states = Array.ofDim[Double](stateCount, stateCount)
    for(i <- 0 to data.size()-1){
      val cluster = clusterer.clusterInstance(data.get(i))
      if(i != 0)
        states(prev)(cluster) += 1
      prev = cluster
      val o = data.get(i).value(colIdx)
      val ind = observations.indexOf(o)
      obs(ind)(cluster) += 1
    }
    (states, obs)
  }

  override def varTypes: List[T] = lens.schema().map(_._2)

  override def sample(seed: Long, idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def sampleGenerator(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def mostLikelyValue(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    val t = util.Date.parse(args(0).asString)
    var n = 1
    var dist = new Matrix(Array[Array[Double]](alpha))
    val transitionMat = new Matrix(transition)
    val emissionMat = new Matrix(emission)
    while((lastTimeStamp + (n*timeDiff)) < t){
      dist = dist.times(transitionMat)
      n += 1
    }
    dist = dist.times(emissionMat.transpose())
    val max = dist.getArray()(0).max
    val res = observations(dist.getArray()(0).indexOf(max))
    FloatPrimitive(res)
  }

  override def upperBoundExpr(idx: Int, args: List[Expression]): Expression = TemporalAnalysis(this, idx, args)

  override def upperBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def sampleGenExpr(idx: Int, args: List[Expression]): Expression = TemporalAnalysis(this, idx, args)

  override def mostLikelyExpr(idx: Int, args: List[Expression]): Expression = TemporalAnalysis(this, idx, args)

  override def lowerBoundExpr(idx: Int, args: List[Expression]): Expression = TemporalAnalysis(this, idx, args)

  override def lowerBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def reason(idx: Int, args: List[Expression]): (String, String) =
    ("I calculated the value at _ will be _ with probability _", "TEMPORAL")
}