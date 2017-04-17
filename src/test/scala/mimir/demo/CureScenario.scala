package mimir.demo

import java.io._

import mimir.Mimir.output
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment, Fragments}
import mimir.test._
import mimir.util._

object CureScenario
  extends SQLTestSpecification("CureScenario",  Map("reset" -> "YES"))
{

  val dataFiles = List(
    new File("test/data/cureSource.csv"),
    new File("test/data/cureLocations.csv"),
    new File("test/data/curePorts.csv")
  )

  def time[A](description: String, op: () => A): A = {
    val t:StringBuilder = new StringBuilder()
    TimeUtils.monitor(description, op, println(_)) 
  }

  sequential 

  "The CURE Scenario" should {
    Fragment.foreach(dataFiles){ table => {
      s"Load '$table'" >> {
        time(
          s"Load '$table'",
          () => { db.loadTable(table) }
        )
        ok
      }
    }}
    "Run the CURE Lenses" >> {
      System.out.println("Running CURE query")
      time("CURE TI Query",
        () => {
          update("CREATE LENS CURE_TI AS SELECT * FROM CURESOURCE_RAW WITH TYPE_INFERENCE(0.6);")
        }
      )
      time("CURE MV Query",
        () => {
          update("CREATE LENS CURE_MV AS SELECT * FROM CURE_TI WITH MISSING_VALUE('IMO_CODE');")
        }
      )

      var table = "CURE_MV"
      val results = db.query(s"SELECT Vessel,JSON_GROUP_ARRAY(distinct IMO_CODE) FROM $table group by Vessel")
      output.print(results)

      /*
      var table = "CURE_MV"
      println(db.getTableSchema(table))
      var schema = db.printSchema(table)
      schema = schema.replace("IMO_CODE,","")
      schema = "IMO_CODE,"+ schema
      println(schema)
      val results = db.query(s"SELECT $schema FROM $table")
      output.print(results)
      */
      ok
    }



/*
    "Run the CURE Query" >> {
      System.out.println("Running CURE query")
      time("CURE Query",
        () => {
          update("""
                          CREATE LENS CURE_TI AS
                    SELECT
                            BILL_OF_LADING_NBR,
                            SRC.IMO_CODE           AS "SRC_IMO",
                            LOC.LAT                AS "VESSEL_LAT",
                            LOC.LON                AS "VESSEL_LON",
                            PORTS.LAT              AS "PORT_LAT",
                            PORTS.LON              AS "PORT_LON",
                            DATE(SRC.DATE)          AS "SRC_DATE",
                            DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON) AS "DISTANCE",  SPEED(DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON), SRC.DATE, NULL) AS "SPEED"
                          FROM CURESOURCE_RAW AS SRC
                           LEFT JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
                            LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT

                           WITH TYPE_INFERENCE(0.8)
                          ;
                 """)
        }

        //         failed type detection --> run type inferencing
        //         --> repair with repairing tool
      )
      ok
    }
*/


    "Select from the source table" >> {
      time("Type Inference Query", 
        () => {
          query("""
            SELECT * FROM cureSource;
          """).foreachRow((x) => {})
        }
      )
      ok
    }

    "Create the source MV Lens" >> {
      time("Source MV Lens",
        () => {
          update("""
            CREATE LENS MV1 
            AS SELECT * FROM cureSource 
            WITH MISSING_VALUE('IMO_CODE');
          """)
        }
      )
      ok
    }

    "Create the locations MV Lens" >> {
      time("Locations MV Lens",
        () => {
          update("""
            CREATE LENS MV2 
            AS SELECT * FROM cureLocations 
            WITH MISSING_VALUE('IMO_CODE');
          """)
        }
      )
      ok
    }

//    time("Materalize MV1", () => {db.selectInto("MAT_MV1","MV1")})
//    time("Materalize MV2", () => {db.selectInto("MAT_MV2","MV2")})

/*
    time("CURE Query Materalized",
      () => {
        query("""
             SELECT *
             FROM   MAT_MV1 AS source
               JOIN MAT_MV2 AS locations
                       ON source.IMO_CODE = locations.IMO_CODE;
              """).foreachRow((x) => {})
      }
    )
*/

//    true

    /*
SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  (julianday(DATE('now'))-julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  ABS(LOC.LAT - PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  (julianday(DATE('now'))-julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  ABS(LOC.LAT - PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  MINUS(julianday(DATE('now')), julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  MINUS(LOC.LAT, PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  MINUS(DATE('now'), DATE(SRC.DATE))      AS "TIME_DIFF",
  MINUS(LOC.LAT, PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;



SELECT DISTINCT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE(SRC.DATE)          AS "SRC_DATE",
  DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON) AS "DISTANCE",  SPEED(DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON), SRC.DATE, NULL) AS "SPEED"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 100;
     */
  }
}
