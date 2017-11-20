
  package mimir.exec.mode


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
  import mimir.parser.MimirJSqlParser
  import net.sf.jsqlparser.statement.Statement

  import scala.collection.mutable.ArrayBuffer

  /**
    * TupleBundles ( http://dl.acm.org/citation.cfm?id=1376686 ) are a tactic for
    * computing over probabilistic data.  Loosely put, the approach is to compile
    * the query to evaluate simultaneously in N possible worlds.  The results can
    * then be aggregated to produce an assortment of statistics, etc...
    *
    * This class actually wraps three different compilation strategies inspired
    * by tuple bundles, each handling parallelization in a slightly different way
    *
    * * **Long**:  Not technically "TupleBundles".  This approach simply unions
    * *            together a set of results, one per possible world sampled.
    * * **Flat**:  Creates a wide result, splitting each non-deterministic column
    * into a set of columns, one per sample.
    * * **Array**: Like flat, but uses native array types to avoid overpopulating
    * the result schema.
    *
    * At present, only 'Flat' is fully implemented, although a 'Long'-like approach
    * can be achieved by using convertFlatToLong.
    */

  class HeuristicHybridMode(seeds: Seq[Long] = (0l until 10l).toSeq)
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

      val (compiled, nonDeterministicColumns) = compileHeuristicHybrid(query, db)
      query = compiled
      query = db.views.resolve(query)
      (
        query,
        //TO-DO check if this is right
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


    def PopulateJoin(ex: Expression, query: Operator) : (String,String)={
      query match {
        case (Table(_, _, _, _) | EmptyTable(_)) => {
          var tableName = (query.asInstanceOf[Table]).getTableName

          (tableName,ex.toString())
        }
        case Project(columns, oldChild) => {
           var (nameL,expL) = ("","")
          //var exp = ""

          columns.map{col=>
            if(ex.toString.equals(col.getName)){
              var (name,exp) = PopulateJoin(col.getRHSExpression,oldChild)
              nameL = name
              expL = exp
            }

          }

          (nameL,expL)

        }

        case Select(condition, oldChild) => {

          PopulateJoin(ex,oldChild)
        }
        case Join(lhsOldChild, rhsOldChild) => {
          var found = false
          var (nameL,expL) = ("","")
          //var name = ""
          //var exp = ""
          var projectArgs = lhsOldChild.columnNames.map{ cols =>
            ProjectArg(cols,Var(cols))
          }
          projectArgs.map{col=>

            if(ex.toString.equals(col.getName)){
              lhsOldChild match {
                case Project(columns,oldChildLHS) =>{
                  found = true
                 var (name, exp) = PopulateJoin(col.getRHSExpression,oldChildLHS)
                   nameL=name
                 expL = exp
                }
              }

            }
          }
          if(!found){
            var projectArgs = rhsOldChild.columnNames.map{ cols =>
              ProjectArg(cols,Var(cols))
            }
            projectArgs.map{col=>
              if(ex.toString.equals(col.getName)){
                rhsOldChild match {
                  case Project(columns,oldChildRHS) =>{
                    var (name, exp)= PopulateJoin(col.getRHSExpression,oldChildRHS)
                    nameL=name
                  expL = exp
                  }
                }

              }
            }

          }

          (nameL, expL)
          }
        case Union(lhsOldChild, rhsOldChild) => {
          //var name = ""
          //var exp = ""
          var found = false
          var (nameL,expL) = ("","")
          var projectArgs = lhsOldChild.columnNames.map{ cols =>
            ProjectArg(cols,Var(cols))
          }
          projectArgs.map{col=>
            if(ex.toString.equals(col.getName)){
              lhsOldChild match {
                case Project(columns,oldChildLHS) =>{
                  found = true
                  var (name, exp) = PopulateJoin(col.getRHSExpression,oldChildLHS)
                  nameL=name
                expL = exp
                }
              }

            }
          }
          if(!found){
            var projectArgs = rhsOldChild.columnNames.map{ cols =>
              ProjectArg(cols,Var(cols))
            }
            projectArgs.map{col=>
              if(ex.toString.equals(col.getName)){
                rhsOldChild match {
                  case Project(columns,oldChildRHS) =>{
                    found = true
                    var (name, exp) = PopulateJoin(col.getRHSExpression,oldChildRHS)
                    nameL=name
                  expL = exp
                  }
                }

              }
            }


          }
          (nameL, expL)

        }

        case Aggregate(gbColumns, aggColumns, oldChild) => {
          PopulateJoin(ex,oldChild)


        }
        case View(_, query, _) => {
          PopulateJoin(ex, query)


        }

      }
    }

    def DetermineJoins(condition: Expression, query: Operator) {
      condition match {
        case p: PrimitiveValue => p

        case Arithmetic(op, lhs, rhs) => {
          DetermineJoins(lhs, query)
          DetermineJoins(rhs, query)
        }
        case Comparison(op, lhs, rhs) => {
          var isJoin = false
          lhs match {
            case Var(_) => isJoin = true
          }
          rhs match {
            case Var(_) =>
            case (_) => isJoin = false
          }
          if (isJoin) {

            val (lhstable,lhsexp)= PopulateJoin(lhs, query)
            val (rhstable,rhsexp) = PopulateJoin(rhs, query)

            val joininfo = new JoinInfo(lhstable,rhstable,lhsexp,rhsexp)
            listOfJoins += joininfo
            return
          }
        }

      }

    }

    def IsNonDeterministic(query: Operator,nonDeterministicInput: Set[String]) : Boolean ={
        query match {
          case (Table(_, _, _, _) | EmptyTable(_)) => {
            var tableName = (query.asInstanceOf[Table]).getTableName
            var schema  = (query.asInstanceOf[Table]).getSchema
            var alias  = (query.asInstanceOf[Table]).getAlias

            println(nonDeterministicInput)
            listOfJoins.map{
              col =>
              println(col.getCol1 +""+col.getTable1)
              println(col.getCol2 +""+col.getTable1)

              if(col.getTable1.equals(tableName)){
                for(i <- 0 until schema.length ){
                  var cols = col.getCol1;
                  if(alias!=null){
                    cols = alias+"_"+cols
                  }
                  println(cols)
                  if((schema(i)_1).equals(col.getCol1) && nonDeterministicInput.contains(cols)){
                    return true
                  }

                }
                return false;
              }else if(col.getTable2.equals(tableName)){
                for(i <- 0 until schema.length ){
                  var cols = col.getCol1;
                  if(alias!=null){
                    cols = alias+"_"+cols
                  }
                  println(cols)
                  if((schema(i)_1).equals(col.getCol2) && nonDeterministicInput.contains(cols)){
                    return true
                  }
                }
                return false
              }
            }
            (false)
          }
          case Project(columns, oldChild) => {
            (IsNonDeterministic(oldChild,nonDeterministicInput))
          }

          case Select(condition, oldChild) => {
            (IsNonDeterministic(oldChild,nonDeterministicInput))
          }
          case Join(lhsOldChild, rhsOldChild) => {

            lhsOldChild match {
              case Project(columns,oldChildLHS) =>{
                if(IsNonDeterministic(oldChildLHS,nonDeterministicInput))
                  (true)
              }
            }
            rhsOldChild match {
              case Project(columns,oldChildRHS) =>{
                (IsNonDeterministic(oldChildRHS,nonDeterministicInput))
              }
            }


          }
          case Union(lhsOldChild, rhsOldChild) => {
            lhsOldChild match {
              case Project(columns,oldChildLHS) =>{
                if(IsNonDeterministic(oldChildLHS,nonDeterministicInput))
                  (true)
              }
            }
            rhsOldChild match {
              case Project(columns,oldChildRHS) =>{
                (IsNonDeterministic(oldChildRHS,nonDeterministicInput))
              }
            }

          }

          case Aggregate(gbColumns, aggColumns, oldChild) => {
            (IsNonDeterministic(oldChild,nonDeterministicInput))

          }

        }
    }

    def compileHeuristicHybrid(query: Operator, db: Database): (Operator, Set[String],String) = {
      // Check for a shortcut opportunity... if the expression is deterministic, we're done!
      if (CTables.isDeterministic(query)) {
        return (query.addColumn(
          WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
        ), Set[String](),"TB")

      }
      query match {
        case (Table(_, _, _, _) | EmptyTable(_)) => {

          (
            query.addColumn(
              WorldBits.columnName -> IntPrimitive(WorldBits.fullBitVector(seeds.size))
            ),
            Set[String](),"TB"
          )
        }
        case Project(columns, oldChild) => {
          val (newChild, nonDeterministicInput,mode) = compileHeuristicHybrid(oldChild, db)

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

          (replacementProjection, nonDeterministicOutputs.flatten.toSet,"TB")
        }

        case Select(condition, oldChild) => {
          DetermineJoins(condition, query)
          val (newChild, nonDeterministicInput,mode) = compileHeuristicHybrid(oldChild, db)

          if (doesExpressionNeedSplit(condition, nonDeterministicInput)) {
            val replacements = splitExpressionByWorlds(condition, nonDeterministicInput, db.models.get(_))

            val updatedWorldBits =
              Arithmetic(Arith.BitAnd,
                Var(WorldBits.columnName),
                replacements.zipWithIndex.map { case (expr, i) =>
                  Conditional(expr, IntPrimitive(1 << i), IntPrimitive(0))
                }.fold(IntPrimitive(0))(Arithmetic(Arith.BitOr, _, _))
              )

            logger.debug(s"Updated World Bits: \n${updatedWorldBits}")
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
            (Select(condition, newChild), nonDeterministicInput,"TB")
          }
        }

        case Join(lhsOldChild, rhsOldChild) => {
          var (lhsNewChild, lhsNonDeterministicInput,mode) = compileHeuristicHybrid(lhsOldChild, db)
          var (rhsNewChild, rhsNonDeterministicInput.mode) = compileHeuristicHybrid(rhsOldChild, db)

          // To safely join the two together, we need to rename the world-bit columns
          if (IsNonDeterministic(lhsNewChild,lhsNonDeterministicInput)|| IsNonDeterministic(rhsNewChild,rhsNonDeterministicInput)) {
            if (IsNonDeterministic(lhsNewChild,lhsNonDeterministicInput)) {
              var worldQuery = getWorldBitQuery(db)

              var lpArgs = worldQuery.columnNames.map(col =>
                ProjectArg(WorldILBits.columnName, Var(col)))

              var lnewQuery = Project(lpArgs, worldQuery)

              var joinQuery = Join(lnewQuery, lhsNewChild)
              var lnpArgs = joinQuery.columnNames.map { cols =>

                  ProjectArg(cols, Var(cols))


              }
              lnpArgs = lnpArgs.filter(_.getName != WorldBits.columnName)


              lhsNewChild = Project(lnpArgs, joinQuery)
            }
            else {
              lhsNewChild = lhsNewChild.addColumn(
                WorldILBits.columnName -> IntPrimitive(WorldILBits.fullBitVector(seeds.size))
              )
              var lnpArgs = lhsNewChild.columnNames.map { cols =>

                ProjectArg(cols, Var(cols))


              }
              lnpArgs = lnpArgs.filter(_.getName != WorldBits.columnName)


              lhsNewChild = Project(lnpArgs, lhsNewChild)


            }
            if (IsNonDeterministic(rhsNewChild,rhsNonDeterministicInput)) {
              var worldQuery = getWorldBitQuery(db)

              var rpArgs = worldQuery.columnNames.map(col =>
                ProjectArg(WorldILBits.columnName, Var(col)))

              var rnewQuery = Project(rpArgs, worldQuery)

              var joinQuery = Join(rnewQuery, rhsNewChild)
              var rnpArgs = joinQuery.columnNames.map { cols =>
                ProjectArg(cols, Var(cols))
              }
              rnpArgs = rnpArgs.filter(_.getName != WorldBits.columnName)

              rhsNewChild = Project(rnpArgs, joinQuery)
            }
            else {
              rhsNewChild = rhsNewChild.addColumn(
                WorldILBits.columnName -> IntPrimitive(WorldILBits.fullBitVector(seeds.size))
              )
              var lnpArgs = rhsNewChild.columnNames.map { cols =>

                ProjectArg(cols, Var(cols))


              }
              lnpArgs = lnpArgs.filter(_.getName != WorldBits.columnName)


              rhsNewChild = Project(lnpArgs, rhsNewChild)
            }

            val rewrittenJoin =
              OperatorUtils.joinMergingColumns(
                Seq((WorldILBits.columnName,
                  (lhs: Expression, rhs: Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                ),
                lhsNewChild, rhsNewChild
              )

            var completedJoin =
              Select(
                Comparison(Cmp.Neq, Var(WorldILBits.columnName), IntPrimitive(0)),
                rewrittenJoin
              )
            completedJoin = completedJoin.columnNames.map{
              cols = >
              if(cols.equals(WorldILBits.columnName)){
                ProjectArg(WorldBits.columnName,Var(cols))
              }else{
                ProjectArg(cols,Var(cols))
              }
            }

            (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput,"IL")
          }
          else {
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

            (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput,"TB")
          }
        }

        case Union(lhsOldChild, rhsOldChild) => {
          val (lhsNewChild, lhsNonDeterministicInput,mode) = compileHeuristicHybrid(lhsOldChild, db)
          val (rhsNewChild, rhsNonDeterministicInput,mode) = compileHeuristicHybrid(rhsOldChild, db)

          val nonDeterministicOutput =
            lhsNonDeterministicInput ++ rhsNonDeterministicInput
          (
            Union(
              lhsNewChild,
              rhsNewChild
            ),
            nonDeterministicOutput,"TB"
          )
        }

        case Aggregate(gbColumns, aggColumns, oldChild) => {
          val (newChild, nonDeterministicInput,mode) = compileHeuristicHybrid(oldChild, db)

         if(mode.equals("TB")){
            val oneOfTheGroupByColumnsIsNonDeterministic =
              gbColumns.map(_.name).exists(nonDeterministicInput(_))

            if(oneOfTheGroupByColumnsIsNonDeterministic){


              var worldQuery = getWorldBitQuery(db)
              val rewrittenJoin =
                OperatorUtils.joinMergingColumns(
                  Seq( (WorldBits.columnName,
                    (lhs:Expression, rhs:Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
                  ),
                  newChild, worldQuery
                )

              val completedJoin =
                Select(
                  Comparison(Cmp.Neq, Var(WorldBits.columnName), IntPrimitive(0)),
                  rewrittenJoin
                )

              var projectArgs = completedJoin.columnNames.map{ cols =>
                ProjectArg(cols,Var(cols))
              }


              var groupcol = gbColumns.map{

                col =>
                  var nexp =
                    if(nonDeterministicInput.contains(col.name)){
                      var clause1 = (Var(WorldBits.columnName).eq(IntPrimitive(1)),Var(TupleBundle.colNameInSample(col.toString, 0)))
                      var clause2 = (Var(WorldBits.columnName).eq(IntPrimitive(2)),Var(TupleBundle.colNameInSample(col.toString, 1)))
                      var clause3 = (Var(WorldBits.columnName).eq(IntPrimitive(4)),Var(TupleBundle.colNameInSample(col.toString, 2)))
                      var clause4 = (Var(WorldBits.columnName).eq(IntPrimitive(8)),Var(TupleBundle.colNameInSample(col.toString, 3)))
                      var clause5 = (Var(WorldBits.columnName).eq(IntPrimitive(16)),Var(TupleBundle.colNameInSample(col.toString, 4)))
                      var clause6 = (Var(WorldBits.columnName).eq(IntPrimitive(32)),Var(TupleBundle.colNameInSample(col.toString, 5)))
                      var clause7 = (Var(WorldBits.columnName).eq(IntPrimitive(64)),Var(TupleBundle.colNameInSample(col.toString, 6)))
                      var clause8 = (Var(WorldBits.columnName).eq(IntPrimitive(128)),Var(TupleBundle.colNameInSample(col.toString, 7)))
                      var clause9 = (Var(WorldBits.columnName).eq(IntPrimitive(256)),Var(TupleBundle.colNameInSample(col.toString, 8)))
                      var listExp:List[(Expression,Expression)] = List(clause1,clause2,clause3,clause4,clause5,clause6,clause7,clause8,clause9)
                      var nullMap:Map[String,Expression] = Map()
                      (ExpressionUtils.makeCaseExpression(listExp,Var(TupleBundle.colNameInSample(col.toString, 9))))
                    } else {
                      (col)
                    }
                  ProjectArg(col.toString(),nexp)
              }


              var shardedChild = Project(projectArgs++groupcol,completedJoin)


              // Split the aggregate columns.  Because a group-by attribute is uncertain, all
              // sources of uncertainty can be, potentially, non-deterministic.
              // As a result, we convert expressions to aggregates over case statements:
              // i.e., SUM(A) AS A becomes
              // SUM(CASE WHEN inputIsInWorld(1) THEN A ELSE NULL END) AS A_1,
              // SUM(CASE WHEN inputIsInWorld(2) THEN A ELSE NULL END) AS A_2,
              // ...
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
                nonDeterministicOutputs.flatten.toSet,"IL"
              )

            } else {


              // This is the easy case: All of the group-by columns are non-deterministic
              // and we can safely use classical aggregation to compute this expression.

              // As before we may need to split aggregate columns, but here we can first
              // check to see if the aggregate expression depends on non-deterministic
              // values.  If it does not, then we can avoid splitting it.
              val (splitAggregates, nonDeterministicOutputs) =
              aggColumns.map { case AggFunction(name, distinct, args, alias) =>
                if(args.exists(doesExpressionNeedSplit(_, nonDeterministicInput))){
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
                nonDeterministicOutputs.flatten.toSet,"TB"
              )

            }




  //        }
        }

        // We don't handle materialized tuple bundles (at the moment)
        // so give up and drop the view.
        case View(_, query, _) => compileHeuristicHybrid(query, db)

        case Sort(sortCols, oldChild) => {
          val (newChild, nonDeterministicInput) = compileHeuristicHybrid(oldChild, db)
          if (limit) {
            (
              Sort(sortCols, newChild), nonDeterministicInput
            )
          }

          else {


            var SortCols: Seq[SortColumn] = Nil
            var newCol: SortColumn = new SortColumn(Var(WorldBits.columnName), true)
            SortCols :+= newCol
            sortCols.map { col =>
              SortCols :+= col
            }
            (
              Sort(SortCols, newChild), nonDeterministicInput
            )

          }


        }


        case Limit(offset, count, oldChild) => {
          limit = true
          val (newChild, nonDeterministicInput) = compileHeuristicHybrid(oldChild, db)
          limit = false
          var projectArgs = newChild.columnNames.map { cols =>
            ProjectArg(cols, Var(cols))
          }
          val sampleShards = (0 until 10).map { i =>
            Project(projectArgs, Limit(offset, count, Select(
              Comparison(Cmp.Eq, Var(WorldBits.columnName), IntPrimitive(Math.pow(2, i).toLong))
              , Project(projectArgs, newChild))))
          }


          (
            OperatorUtils.makeUnion(sampleShards), nonDeterministicInput
          )

        }
        case (LeftOuterJoin(_, _, _) | Annotate(_, _) | ProvenanceOf(_) | Recover(_, _)) =>
          throw new RAException("Tuple-Bundler presently doesn't support LeftOuterJoin, Sort, or Limit (probably need to resort to 'Long' evaluation)")
      }
    }

  }
