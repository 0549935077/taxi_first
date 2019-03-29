package cads

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}


object Start {


  def main(args: Array[String]): Unit = {
    val ID: String = "id"
    val CITY: String = "city"
    val KM: String = "km"
    val FULL_NAME: String = "fullName"

    val conf = new SparkConf().setAppName("taxisc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder.getOrCreate
    val dataSetTrips = session.read.json("data/orders.txt")
    val dataSetDrivers = session.read.json("data/drivers_j.txt")
    dataSetTrips.show()
    dataSetDrivers.show()

    // *** count of trips
    val countTrips = dataSetTrips.count()
    println(s"count: $countTrips")

    //  ******  amount trips in cities  **************
    val amountTrips: Dataset[Row] = dataSetTrips.groupBy(CITY)
      .agg(functions.count(CITY))
    amountTrips.show()

    // ******  longest trip in every city  **************
    val longTrip: Dataset[Row] = dataSetTrips.groupBy(CITY)
      .agg(functions.last(KM))
    longTrip.show()

    // ******** trip for each driver ************
    val tripEachDriver: Dataset[Row] = dataSetTrips.groupBy(ID)
      .agg(functions.sum(KM))
    tripEachDriver.show()


    // driver who has the largest number of trips with the same distance

    dataSetTrips.createOrReplaceTempView("dataSetTrips")
    dataSetDrivers.createOrReplaceTempView("dataSetDrivers")

    val distanceDrivers = session.sql("SELECT * FROM dataSetTrips " +
      "INNER JOIN dataSetDrivers ON dataSetTrips.id" + " = dataSetDrivers.id")
    distanceDrivers.show()

    val driverKm: Dataset[Row] = distanceDrivers.select(KM,FULL_NAME)
    driverKm.show()

    driverKm.createOrReplaceTempView("driverKm")
    val driversHavingSameTrips: Dataset[Row] = session.sql("select * from driverKm" +
      " where km in (select km from driverKm group by km having count(*) > 1) ")
    driversHavingSameTrips.show()

    val countSameTrips: Dataset[Row] = driversHavingSameTrips.groupBy(FULL_NAME)
      .agg(functions.count(KM).as(KM))
    countSameTrips.show()

    val drivers: Dataset[Row] = countSameTrips.orderBy(functions.desc(KM))
    drivers.show();

    val driver  = drivers.first()
    println(s"driver who has the largest number of trips with the same distance: $driver")

  }

}







