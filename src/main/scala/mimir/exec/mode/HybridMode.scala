
package mimir.exec.mode

import mimir.exec.mode.JoinInfo
import java.io.ByteArrayInputStream

import mimir.Database
import mimir.optimizer.operator._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.exec._
import mimir.exec.result._
import mimir.models.Model
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.ctables.CTables.isProbabilistic
import mimir.parser.MimirJSqlParser
import net.sf.jsqlparser.statement.Statement

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue,Stack}


class HybridMode(seeds: Seq[Long] = (0l until 10l).toSeq, stack: scala.collection.mutable.Stack[String])
  extends CompileMode[SampleResultIterator]
    with LazyLogging {
  var limit = false
  var listOfJoins = ArrayBuffer[JoinInfo]()
  type MetadataT =
    (
      Set[String], // Nondeterministic column set
        Seq[String] // Provenance columns
      )


  def rewrite(db: Database, queryRaw: Operator): (Operator, Seq[String], MetadataT) = {
    var query = queryRaw
    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance
    val (compiled, nonDeterministicColumns, mode) = compileHeuristicHybrid(query, db, stack)
    query = compiled
    query = db.views.resolve(query)
    println(query)
    (
      query,
      query.columnNames,
      (nonDeterministicColumns, provenanceCols)
    )
  }


  def getWorldBitQuery(db: Database): Operator = {

    if (!db.tableExists("WORLDBits")) {
      db.backend.update(
        s"""
        CREATE TABLE WORLDBits(
          MIMIR_WORLD_BITS int,
          PRIMARY KEY (MIMIR_WORLD_BITS)
        )
      """)
      val bits = List(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
      db.backend.fastUpdateBatch(
        s"""
        INSERT INTO WORLDBits (MIMIR_WORLD_BITS) VALUES (?);
      """,
        bits.map { bit =>
          Seq(IntPrimitive(bit))
        }
      )
    }
    var queryString = "select * from WORLDBits"
    var parser = new MimirJSqlParser(new ByteArrayInputStream(queryString.getBytes));
    val stmt: Statement = parser.Statement();
    (db.sql.convert(stmt.asInstanceOf[net.sf.jsqlparser.statement.select.Select]))
  }

  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): SampleResultIterator = {
    new SampleResultIterator(
      results,
      db.typechecker.schemaOf(query),
      meta._1,
      seeds.size
    )
  }

  def doesExpressionNeedSplit(expression: Expression, nonDeterministicInputs: Set[String]): Boolean = {
    val allInputs = ExpressionUtils.getColumns(expression)
    val expressionHasANonDeterministicInput =
      allInputs.exists {
        nonDeterministicInputs(_)
      }
    val expressionIsNonDeterministic =
      !CTables.isDeterministic(expression)

    return expressionHasANonDeterministicInput || expressionIsNonDeterministic
  }

  def splitExpressionsByWorlds(expressions: Seq[Expression], nonDeterministicInputs: Set[String], models: (String => Model)): Seq[Seq[Expression]] = {
    val outputColumns =
      seeds.zipWithIndex.map { case (seed, i) =>
        val inputInstancesInThisSample =
          nonDeterministicInputs.
            map { x => (x -> Var(TupleBundle.colNameInSample(x, i))) }.
            toMap
        expressions.map { expression =>
          CTAnalyzer.compileSample(
            Eval.inline(expression, inputInstancesInThisSample),
            IntPrimitive(seed),
            models
          )
        }
      }

    outputColumns
  }

  def splitExpressionByWorlds(expression: Expression, nonDeterministicInputs: Set[String], models: (String => Model)): Seq[Expression] = {
    splitExpressionsByWorlds(Seq(expression), nonDeterministicInputs, models).map(_ (0))
  }

  def compileHeuristicHybrid(query: Operator, db: Database, queueOfApproaches: Stack[String]): (Operator, Set[String], String) = {
    // Check for a shortcut opportunity... if the expression is deterministic, we're done!
    if (CTables.isDeterministic(query)) {
      var mode = queueOfApproaches.pop();
      if (mode.equals("IL")) {
        var worldQuery = getWorldBitQuery(db)
        var joinQuery = Join(query, worldQuery)
        var projectArgs = joinQuery.columnNames.map { cols =>
          ProjectArg(cols, Var(cols))
        }
        var projQuery = Project(projectArgs, joinQuery)

        return (
          projQuery,
          Set[String](), "IL"
        )
      }
      else {

        return (query.addColumn(
          WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
        ), Set[String](), "TB")
      }

    }
    query match {
      case (Table(_, _, _, _) | EmptyTable(_)) => {
        (
          query.addColumn(
            WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
          ),
          Set[String](), "TB"
        )
      }
      case Project(columns, oldChild) => {
        val (newChild, nonDeterministicInput, mode) = compileHeuristicHybrid(oldChild, db, queueOfApproaches)
        //POP from stack returned by query optimizer and use mode
        var qmode = queueOfApproaches.pop();
        if (mode.equals("IL")) {
          val (
            newColumns,
            nonDeterministicOutputs
            ): (Seq[ProjectArg], Seq[Set[String]]) = columns.map { col =>
            if (!CTables.isDeterministic(col.expression)) {
              var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)), IntPrimitive(seeds(0)))
              var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)), IntPrimitive(seeds(1)))
              var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)), IntPrimitive(seeds(2)))
              var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)), IntPrimitive(seeds(3)))
              var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)), IntPrimitive(seeds(4)))
              var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)), IntPrimitive(seeds(5)))
              var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)), IntPrimitive(seeds(6)))
              var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)), IntPrimitive(seeds(7)))
              var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)), IntPrimitive(seeds(8)))
              var listExp: List[(Expression, Expression)] = List(clause1, clause2, clause3, clause4, clause5, clause6, clause7, clause8, clause9)
              var nullMap: Map[String, Expression] = Map()
              (ProjectArg(col.name,
                CTAnalyzer.compileSample(
                  Eval.inline(col.expression, nullMap),
                  ExpressionUtils.makeCaseExpression(listExp, IntPrimitive(seeds(9))),
                  db.models.get(_)
                )),
                Set(col.name)
              )
            } else {
              (col, Set[String]())
            }
          }.unzip

          val replacementProjection =
            Project(
              newColumns ++ Seq(ProjectArg(WorldBits.columnName, Var(WorldBits.columnName))),
              newChild
            )
          (replacementProjection, nonDeterministicOutputs.flatten.toSet, mode)

        } else {
          val (
            newColumns,
            nonDeterministicOutputs
            ): (Seq[Seq[ProjectArg]], Seq[Set[String]]) = columns.map { col =>
            if (doesExpressionNeedSplit(col.expression, nonDeterministicInput)) {
              (
                splitExpressionByWorlds(col.expression, nonDeterministicInput, db.models.get(_)).
                  zipWithIndex
                  map { case (expr, i) => ProjectArg(TupleBundle.colNameInSample(col.name, i), expr) },
                Set(col.name)
              )
            } else {
              (Seq(col), Set[String]())
            }
          }.unzip

          val replacementProjection =
            Project(
              newColumns.flatten ++ Seq(ProjectArg(WorldBits.columnName, Var(WorldBits.columnName))),
              newChild
            )


          (replacementProjection, nonDeterministicOutputs.flatten.toSet, mode)

        }


      }

      case Select(condition, oldChild) => {

        val (newChild, nonDeterministicInput, mode) = compileHeuristicHybrid(oldChild, db, queueOfApproaches)

        var qmode = queueOfApproaches.pop();
        if (mode.equals("IL")) {
          (Select(condition, newChild), nonDeterministicInput, mode)

        } else {
          if(qmode.equals("TB")){
            if(doesExpressionNeedSplit(condition, nonDeterministicInput)){
              val replacements = splitExpressionByWorlds(condition, nonDeterministicInput, db.models.get(_))

              val updatedWorldBits =
                Arithmetic(Arith.BitAnd,
                  Var(WorldBits.columnName),
                  replacements.zipWithIndex.map { case (expr, i) =>
                    Conditional(expr, IntPrimitive(1 << i), IntPrimitive(0))
                  }.fold(IntPrimitive(0))(Arithmetic(Arith.BitOr, _, _))
                )

              //logger.debug(s"Updated World Bits: \n${updatedWorldBits}")
              val newChildWithUpdatedWorldBits =
                OperatorUtils.replaceColumn(
                  WorldBits.columnName,
                  updatedWorldBits,
                  newChild
                )
              (
                Select(
                  Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
                  newChildWithUpdatedWorldBits
                ),
                nonDeterministicInput,"TB"
              )
            } else {
              ( Select(condition, newChild), nonDeterministicInput, "TB" )
            }
          }else{
            var worldQuery = getWorldBitQuery(db)
            val rewrittenJoin =
              OperatorUtils.joinMergingColumns(
                Seq((WorldBits.columnName,
                  (lhs: Expression, rhs: Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                ),
                newChild, worldQuery
              )

            val completedJoin =
              Select(
                Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
                rewrittenJoin
              )

            var projectArgs = completedJoin.columnNames.map { cols =>
              ProjectArg(cols, Var(cols))
            }

            var donelist: mutable.HashSet[String] = new mutable.HashSet()
            var nProjections:ArrayBuffer[ProjectArg] = new ArrayBuffer[ProjectArg]
            projectArgs.map {
              col =>

                  if (col.getName.startsWith("MIMIR_SAMPLE_")) {
                    var name = col.getName.substring(15)
                    if (!donelist.contains(name)) {
                      donelist+=name
                      var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)), Var(TupleBundle.colNameInSample(name, 0)))
                      var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)), Var(TupleBundle.colNameInSample(name, 1)))
                      var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)), Var(TupleBundle.colNameInSample(name, 2)))
                      var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)), Var(TupleBundle.colNameInSample(name, 3)))
                      var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)), Var(TupleBundle.colNameInSample(name, 4)))
                      var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)), Var(TupleBundle.colNameInSample(name, 5)))
                      var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)), Var(TupleBundle.colNameInSample(name, 6)))
                      var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)), Var(TupleBundle.colNameInSample(name, 7)))
                      var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)), Var(TupleBundle.colNameInSample(name, 8)))
                      var listExp: List[(Expression, Expression)] = List(clause1, clause2, clause3, clause4, clause5, clause6, clause7, clause8, clause9)
                      var nullMap: Map[String, Expression] = Map()
                      var nExp = ExpressionUtils.makeCaseExpression(listExp, Var(TupleBundle.colNameInSample(name, 9)))
                      nProjections+=ProjectArg(col.getName.substring(15), nExp)

                    }

                  } else {
                    var nexp =(col.getName)
                  }
            }


            var shardedChild = Project(projectArgs++nProjections, completedJoin)
            (Select(condition, shardedChild), nonDeterministicInput, "IL")

          }

        }

      }

      case Join(lhsOldChild, rhsOldChild) => {
        var (lhsNewChild, lhsNonDeterministicInput, lmode) = compileHeuristicHybrid(lhsOldChild, db, queueOfApproaches)
        var (rhsNewChild, rhsNonDeterministicInput, rmode) = compileHeuristicHybrid(rhsOldChild, db, queueOfApproaches)
        //POP from stack returned by query optimizer and use mode
        var mode = queueOfApproaches.pop();
        // To safely join the two together, we need to rename the world-bit columns

        val rewrittenJoin =
          OperatorUtils.joinMergingColumns(
            Seq((WorldBits.columnName,
              (lhs: Expression, rhs: Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
            ),
            lhsNewChild, rhsNewChild
          )

        // Finally, add a selection to filter out values that can be filtered out in all worlds.
        val completedJoin =
          Select(
            Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
            rewrittenJoin
          )

        if (mode.equals("IL"))
          (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput, "IL")
        else
          (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput, "TB")
      }

      case Union(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput, lmode) = compileHeuristicHybrid(lhsOldChild, db, queueOfApproaches)
        val (rhsNewChild, rhsNonDeterministicInput, rmode) = compileHeuristicHybrid(rhsOldChild, db, queueOfApproaches)
        var mode = queueOfApproaches.pop();

        val schema = query.columnNames

        val alignNonDeterminism = (
                                    query: Operator,
                                    nonDeterministicInput: Set[String],
                                    nonDeterministicOutput: Set[String]
                                  ) => {
          Project(
            schema.flatMap { col =>
              if (nonDeterministicOutput(col)) {
                if (nonDeterministicInput(col)) {
                  WorldBits.sampleCols(col, seeds.size).map { sampleCol => ProjectArg(sampleCol, Var(sampleCol)) }
                } else {
                  WorldBits.sampleCols(col, seeds.size).map { sampleCol => ProjectArg(sampleCol, Var(col)) }
                }
              } else {
                if (nonDeterministicInput(col)) {
                  throw new RAException("ERROR: Non-deterministic inputs must produce non-deterministic outputs")
                } else {
                  Seq(ProjectArg(col, Var(col)))
                }
              }
            },
            query
          )
        }

        val nonDeterministicOutput =
          lhsNonDeterministicInput ++ rhsNonDeterministicInput
        (
          Union(
            alignNonDeterminism(lhsNewChild, lhsNonDeterministicInput, nonDeterministicOutput),
            alignNonDeterminism(rhsNewChild, rhsNonDeterministicInput, nonDeterministicOutput)
          ),
          nonDeterministicOutput, mode
        )
      }

      case Aggregate(gbColumns, aggColumns, oldChild) => {
        val (newChild, nonDeterministicInput, mode) = compileHeuristicHybrid(oldChild, db, queueOfApproaches)
        var qmode = queueOfApproaches.pop();
        if (mode.equals("TB")) {
          // val oneOfTheGroupByColumnsIsNonDeterministic =
          //   gbColumns.map(_.name).exists(nonDeterministicInput(_))
          if (qmode.equals("IL")) {
            var worldQuery = getWorldBitQuery(db)
            val rewrittenJoin =
              OperatorUtils.joinMergingColumns(
                Seq((WorldBits.columnName,
                  (lhs: Expression, rhs: Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                ),
                newChild, worldQuery
              )

            val completedJoin =
              Select(
                Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
                rewrittenJoin
              )

            var projectArgs = completedJoin.columnNames.map { cols =>
              ProjectArg(cols, Var(cols))
            }


            var donelist: mutable.HashSet[String] = new mutable.HashSet()
            var nProjections:ArrayBuffer[ProjectArg] = new ArrayBuffer[ProjectArg]
            projectArgs.map {
              col =>
                if (col.getName.startsWith("MIMIR_SAMPLE_")) {
                  var name = col.getName.substring(15)
                  if (!donelist.contains(name)) {
                    donelist+=name
                    var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)), Var(TupleBundle.colNameInSample(name, 0)))
                    var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)), Var(TupleBundle.colNameInSample(name, 1)))
                    var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)), Var(TupleBundle.colNameInSample(name, 2)))
                    var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)), Var(TupleBundle.colNameInSample(name, 3)))
                    var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)), Var(TupleBundle.colNameInSample(name, 4)))
                    var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)), Var(TupleBundle.colNameInSample(name, 5)))
                    var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)), Var(TupleBundle.colNameInSample(name, 6)))
                    var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)), Var(TupleBundle.colNameInSample(name, 7)))
                    var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)), Var(TupleBundle.colNameInSample(name, 8)))
                    var listExp: List[(Expression, Expression)] = List(clause1, clause2, clause3, clause4, clause5, clause6, clause7, clause8, clause9)
                    var nullMap: Map[String, Expression] = Map()
                    var nexp = ExpressionUtils.makeCaseExpression(listExp, Var(TupleBundle.colNameInSample(name, 9)))
                    nProjections+=ProjectArg(col.getName.substring(15), nexp)
                  }

                } else {
                  var nexp =(col.getName)
                }
            }

            var shardedChild = Project(projectArgs ++ nProjections, completedJoin)

            val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              val splitAggregates =
                (0 until seeds.size).map { i =>
                  AggFunction(name, distinct,
                    args.map { arg =>
                      Conditional(
                        Comparison(Cmp.Eq,
                          Arithmetic(Arith.BitAnd,
                            Var(WorldBits.columnName),
                            IntPrimitive(1 << i)
                          ),
                          IntPrimitive(1 << i)
                        ),
                        arg,
                        NullPrimitive()
                      )
                    },
                    TupleBundle.colNameInSample(alias, i)
                  )
                }
              (splitAggregates, Set(alias))
            }.unzip

            // We also need to figure out which worlds each group will be present in.
            // We take an OR of all of the worlds that lead to the aggregate being present.
            val worldBitsAgg =
            AggFunction("GROUP_BITWISE_OR", false, Seq(Var(WorldBits.columnName)), WorldBits.columnName)

            (
              Aggregate(gbColumns, splitAggregates.flatten ++ Seq(worldBitsAgg), shardedChild),
              nonDeterministicOutputs.flatten.toSet, "IL"
            )

          } else {
            // This is the easy case: All of the group-by columns are non-deterministic
            // and we can safely use classical aggregation to compute this expression.

            // As before we may need to split aggregate columns, but here we can first
            // check to see if the aggregate expression depends on non-deterministic
            // values.  If it does not, then we can avoid splitting it.
            val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              if (args.exists(doesExpressionNeedSplit(_, nonDeterministicInput))) {
                val splitAggregates =
                  splitExpressionsByWorlds(args, nonDeterministicInput, db.models.get(_)).
                    zipWithIndex.
                    map { case (newArgs, i) => AggFunction(name, distinct, newArgs, TupleBundle.colNameInSample(alias, i)) }
                (splitAggregates, Set(alias))
              } else {
                (Seq(AggFunction(name, distinct, args, alias)), Set[String]())
              }
            }.unzip

            // Same deal as before: figure out which worlds the group will be present in.

            val worldBitsAgg =
              AggFunction("GROUP_BITWISE_OR", false, Seq(Var(WorldBits.columnName)), WorldBits.columnName)

            (
              Aggregate(gbColumns, splitAggregates.flatten ++ Seq(worldBitsAgg), newChild),
              nonDeterministicOutputs.flatten.toSet, "TB"
            )

          }


        } else {
          (
            Aggregate(gbColumns ++ Seq(Var(WorldBits.columnName)), aggColumns,
              newChild), nonDeterministicInput, "IL"
          )
        }

      }

      // We don't handle materialized tuple bundles (at the moment)
      // so give up and drop the view.
      case View(_, query, _) => {
        compileHeuristicHybrid(query, db, queueOfApproaches)
      }

      case Sort(sortCols, oldChild) => {
        val (newChild, nonDeterministicInput, mode) = compileHeuristicHybrid(oldChild, db, queueOfApproaches)
        var qmode = queueOfApproaches.pop();
        if (mode.equals("TB")) {
          // val oneOfTheOrderByColumnsIsNonDeterministic =
          //   sortCols.map(_.expression.toString()).exists(nonDeterministicInput(_))

          if (qmode.equals("TB")) {
            (
              Sort(sortCols, newChild), nonDeterministicInput, "TB"
            )
          }

          else {

            var worldQuery = getWorldBitQuery(db)
            val rewrittenJoin =
              OperatorUtils.joinMergingColumns(
                Seq((WorldBits.columnName,
                  (lhs: Expression, rhs: Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                ),
                newChild, worldQuery
              )

            val completedJoin =
              Select(
                Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
                rewrittenJoin
              )

            var projectArgs = completedJoin.columnNames.map { cols =>
              ProjectArg(cols, Var(cols))
            }



            var donelist: mutable.HashSet[String] = new mutable.HashSet()
            var nProjections:ArrayBuffer[ProjectArg] = new ArrayBuffer[ProjectArg]
            projectArgs.map {
              col =>


                if (col.getName.startsWith("MIMIR_SAMPLE_")) {
                  var name = col.getName.substring(15)
                  if (!donelist.contains(name)) {
                    donelist+=name
                    var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)), Var(TupleBundle.colNameInSample(name, 0)))
                    var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)), Var(TupleBundle.colNameInSample(name, 1)))
                    var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)), Var(TupleBundle.colNameInSample(name, 2)))
                    var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)), Var(TupleBundle.colNameInSample(name, 3)))
                    var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)), Var(TupleBundle.colNameInSample(name, 4)))
                    var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)), Var(TupleBundle.colNameInSample(name, 5)))
                    var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)), Var(TupleBundle.colNameInSample(name, 6)))
                    var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)), Var(TupleBundle.colNameInSample(name, 7)))
                    var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)), Var(TupleBundle.colNameInSample(name, 8)))
                    var listExp: List[(Expression, Expression)] = List(clause1, clause2, clause3, clause4, clause5, clause6, clause7, clause8, clause9)
                    var nullMap: Map[String, Expression] = Map()
                    var nexp = ExpressionUtils.makeCaseExpression(listExp, Var(TupleBundle.colNameInSample(name, 9)))
                    nProjections+=ProjectArg(col.getName.substring(15), nexp)

                  }

                } else {
                  var nexp =(col.getName)
                }
            }

            var shardedChild = Project(projectArgs ++ nProjections, completedJoin)

            var SortCols: Seq[SortColumn] = Nil
            var newCol: SortColumn = new SortColumn(Var(WorldBits.columnName), true)
            SortCols :+= newCol
            sortCols.map { col =>
              SortCols :+= col
            }

            (
              Sort(SortCols, shardedChild), nonDeterministicInput, "IL"
            )

          }
        } else {
            var SortCols: Seq[SortColumn] = Nil
            var newCol: SortColumn = new SortColumn(Var(WorldBits.columnName), true)
            SortCols :+= newCol
            sortCols.map { col =>
              SortCols :+= col
            }

            (
              Sort(SortCols, newChild), nonDeterministicInput, "IL"
            )

        }


      }


      case Limit(offset, count, oldChild) => {
        val (newChild, nonDeterministicInput, mode) = compileHeuristicHybrid(oldChild, db, queueOfApproaches)
        var qmode = queueOfApproaches.pop();

        (
          Limit(offset, count, newChild), nonDeterministicInput, mode
        )

      }
      case (LeftOuterJoin(_, _, _) | Annotate(_, _) | ProvenanceOf(_) | Recover(_, _)) =>
        throw new RAException("Tuple-Bundler presently doesn't support LeftOuterJoin, Sort, or Limit (probably need to resort to 'Long' evaluation)")
    }
  }

}
