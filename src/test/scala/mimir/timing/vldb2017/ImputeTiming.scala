package mimir.timing.vldb2017

import java.io._
import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._
import scala.concurrent.duration._

import mimir.parser.MimirJSqlParser
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody}

import mimir.algebra._
import mimir.util._
import mimir.ctables.InlineVGTerms
import mimir.optimizer.operator.InlineProjections
import mimir.test.{SQLTestSpecification, PDBench, TestTimer}
import mimir.models._
import mimir.exec.uncertainty._

object ImputeTiming
  extends VLDB2017TimingTest("tpch10_tpch_UC1", Map("reset" -> "NO", "inline" -> "YES"))//, "initial_db" -> "test/tpch-impute-1g.db"))
  with BeforeAll
{

  sequential

  val fullReset = false
  val runBestGuessQueries = false
  val runTupleBundleQueries = true
  val runSamplerQueries = false
  val useMaterialized = false
  val useFastPathCache = false

  val timeout = 15.minute

  def beforeAll =
  {
    if(fullReset){
      println("DELETING ALL MIMIR METADATA")
      update("DELETE FROM MIMIR_MODEL_OWNERS")
      update("DELETE FROM MIMIR_MODELS")
      update("DELETE FROM MIMIR_VIEWS")
    }
  }

  val relevantTables = Seq(
  //  ("CUSTOMER", Seq("NATIONKEY")),
    ("LINEITEM", Seq("linestatus","orderkey","discount")),
  //  ("PARTSUPP", Seq("PARTKEY", "SUPPKEY")),
  //  ("NATION", Seq("REGIONKEY")),
  //  ("SUPPLIER", Seq("NATIONKEY")),
    ("ORDERS", Seq("custkey"))
  )

  val relevantIndexes = Seq(
/*    ("SUPPLIER", Seq(
      Seq("SUPPKEY"),
      Seq("NATIONKEY")
    )),
    ("PARTSUPP", Seq(
      Seq("PARTKEY", "SUPPKEY"),
      Seq("SUPPKEY")
    )),
    ("CUSTOMER", Seq(
      Seq("CUSTKEY"),
      Seq("NATIONKEY")
    )),
    ("NATION", Seq(
      Seq("NATIONKEY")
    )),*/
    ("LINEITEM", Seq(
    //  Seq("ORDERKEY", "LINENUMBER"),
      //Seq("PARTKEY"),
      Seq("shipdate")
    ))
  //  ("ORDERS", Seq(
    //  Seq("ORDERKEY"),
      //Seq("CUSTKEY")
    //))
  )
  if(false){ "Skipping TPCH Inpute Test" >> ok } else {
    "TPCH Impute" should {

      sequential
      Fragments.foreach(1 to 1){ i =>

        val TPCHQueries =
          Seq(
            //
            s"""
select discount from lineitem_run_$i  where quantity > 49 and returnflag = 'R'  and shipmode = 'FOB' and extendedprice>84000 order by discount;
            """"

            // ,
            // s"""
            //   -- TPC-H Query 5
            //   SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue
            //    FROM   region r, nation_run_$i n, supplier_run_$i s, customer_run_$i c, orders_run_$i o, lineitem_run_$i l
            //   WHERE  r.name = 'ASIA'
            //     AND  n.regionkey = r.regionkey
            //     AND  c.custkey = o.custkey
            //     AND  o.orderdate >= DATE('1994-01-01')
            //     AND  o.orderdate <  DATE('1995-01-01')
            //     AND  l.orderkey = o.orderkey
            //     AND  l.suppkey = s.suppkey
            //     AND  c.nationkey = s.nationkey
            //     AND  s.nationkey = n.nationkey
            //   GROUP BY n.name
            // """
            // ,
            // s"""
            //   -- TPC-H Query 9
            //   SELECT nation, o_year, SUM(amount) AS sum_profit
            //   FROM (
            //     SELECT n.name AS nation,
            //            EXTRACT(year from o.orderdate) AS o_year,
            //            ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
            //               AS amount
            //     FROM  part p,
            //           partsupp_run_$i ps,
            //           supplier_run_$i s,
            //           lineitem_run_$i l,
            //           orders_run_$i o,
            //           nation_run_$i n
            //     WHERE  (p.name LIKE '%green%')
            //       AND  ps.partkey = l.partkey
            //       AND  ps.suppkey = l.suppkey
            //       AND  p.partkey = l.partkey
            //       AND  s.suppkey = l.suppkey
            //       AND  o.orderkey = l.orderkey
            //       AND  s.nationkey = n.nationkey
            //     ) AS profit
            //   GROUP BY nation, o_year;
            // """
          )


        sequential

        // CREATE LENSES
        Fragments.foreach(
          relevantTables.toSeq
        ){ createMissingValueLens(_, s"_RUN_$i") }

        // // INDEXES
        if(useMaterialized){
          Fragments.foreach( relevantIndexes ) {
            case (baseTable, indices) =>
              val viewTable = s"${baseTable}_RUN_$i"
              Fragments.foreach(indices){ index =>
                s"Create Index $viewTable(${index.mkString(",")})" >> {
                  val indexName = viewTable+"_"+index.mkString("_")
                  println(s"CREATE INDEX $indexName")
                  db.backend.update(s"CREATE INDEX IF NOT EXISTS $indexName ON $viewTable(${index.mkString(",")})")
                  ok
                }
              }
          }
        } else { "No Need To Create Indexes" >> ok }

        // QUERIES
        if(runBestGuessQueries){
          Fragments.foreach( TPCHQueries.zipWithIndex )
          {
            queryLens(_)
          }
        } else { "Skipping Best Guess Tests" >> ok }

        if(runTupleBundleQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
           case (a,b) => {
             sampleFromLens((a,b))
           }
           }

        } else { "Skipping Tuple Bundle Tests" >> ok }

        if(runSamplerQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
            expectedFromLens(_)
          }
        } else { "Skipping Sampler Tests" >> ok }


      }

    }
  }
}
