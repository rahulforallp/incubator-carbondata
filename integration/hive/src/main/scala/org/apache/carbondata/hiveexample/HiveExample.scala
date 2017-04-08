/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.hiveexample

import java.io.File
import java.sql.{DriverManager, ResultSet, SQLException, Statement}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

object HiveExample {

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  /**
   * @param args
   * @throws SQLException
   */
  @throws[SQLException]
  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val store = s"$rootPath/integration/hive/target/store"
    val warehouse = s"$rootPath/integration/hive/target/warehouse"
    val metaStore_Db = s"$rootPath/integration/hive/target/carbon_metaStore_db"
    val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

    import org.apache.spark.sql.CarbonSession._

    System.setProperty("hadoop.home.dir", "/")

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("HiveExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        store, metaStore_Db)

    val carbonJarPath = s"$rootPath/assembly/target/scala-2.11/carbondata_2.11-1.1" +
                        s".0-incubating-SNAPSHOT-shade-hadoop2.7.2.jar"
    val hiveJarPath = s"$rootPath/integration/hive/target/carbondata-hive-1.1" +
                      s".0-incubating-SNAPSHOT.jar"

    carbon.sql("""drop table if exists hive_carbon_example""".stripMargin)

    carbon
      .sql(
        """create table hive_carbon_example (id int,name string,salary double) stored by
          |'carbondata' """
          .stripMargin)

    carbon.sql(
      s"""
           LOAD DATA LOCAL INPATH '$rootPath/integration/hive/src/main/resources/data.csv' into
           table
         hive_carbon_example
           """)
    carbon.sql("select * from hive_carbon_example").show()

    carbon.stop()

    try {
      Class.forName(driverName)
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        classNotFoundException.printStackTrace()
    }

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2()
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort
    val con = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
    val stmt: Statement = con.createStatement

    logger.info(s"============HIVE CLI IS STARTED ON PORT $port ==============")

    try {
      stmt
        .execute(s"ADD JAR $carbonJarPath")
    }
    catch {
      case exception: Exception => logger.error(s"Exception Occurs:Jar Not Found $carbonJarPath")
        hiveEmbeddedServer2.stop()

    }
    try {
      stmt
        .execute(s"ADD JAR $hiveJarPath")
    }
    catch {
      case exception: Exception => logger.error(s"Exception Occurs:Jar Not Found $hiveJarPath")
        hiveEmbeddedServer2.stop()

    }
    stmt.execute("set hive.mapred.supports.subdirectories=true")
    stmt.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")


    stmt.execute("create table if not exists " + "hive_carbon_example " +
                 " (id int, name string,salary double)")
    stmt
      .execute(
        "alter table hive_carbon_example set FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "alter table hive_carbon_example set LOCATION " +
        s"'file:///$store/default/hive_carbon_example' ")


    val sql = "select * from hive_carbon_example"

    val res: ResultSet = stmt.executeQuery(sql)

    var rowsFetched = 0

    while (res.next) {
      if (rowsFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| id|" + "| name |" + "| salary        |")

        println("+---+" + "+-------+" + "+--------------+")

        val resultId = res.getString("id")
        val resultName = res.getString("name")
        val resultSalary = res.getString("salary")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary  |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        val resultId = res.getString("id")
        val resultName = res.getString("name")
        val resultSalary = res.getString("salary")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary   |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      rowsFetched = rowsFetched + 1
    }
    println(s"******Total Number Of Rows Fetched ****** $rowsFetched")
    hiveEmbeddedServer2.stop()
  }

}