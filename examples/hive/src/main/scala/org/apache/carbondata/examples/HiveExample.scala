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

package org.apache.carbondata.examples

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement}

// Write carbondata file by spark and read it by flink
// scalastyle:off println
object HiveExample {
  val currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  def main(args: Array[String]): Unit = {

    val hiveLocalMetaStore = HiveLocalMetaStore.setup(currentPath)
    println("\n========================successfully Started metastore")
    val hiveLocalServer2 = HiveLocalServer2.setup(currentPath)
    println("\n========================successfully Started HiveServer")

    Thread.sleep(1000)
    println("starting client")
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        // TODO Auto-generated catch block
        classNotFoundException.printStackTrace()
        System.exit(1)
    }
    //replace " " here with the name of the user the queries should run as
    val con: Connection = DriverManager
      .getConnection("jdbc:hive2://localhost:10001/default", "", "")
    val stmt: Statement = con.createStatement
    println("============HIVE CLI IS STARTED=============")

    stmt
      .execute(s"ADD JAR $rootPath/assembly/target/scala-2.11/carbondata_2.11-1.1" +
               s".0-incubating-SNAPSHOT-shade-hadoop2.7.2.jar ")
    stmt
      .execute(s"ADD JAR $rootPath/integration/hive/target/carbondata-hive-1.1" +
               s".0-incubating-SNAPSHOT.jar")

    stmt.execute("create table if not exists " + "hive_carbon_example " +
                 " (id int, name string)")

    stmt
      .execute(
        "alter table hive_carbon_example set FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "alter table hive_carbon_example set LOCATION " +
        "'hdfs://localhost:54310/opt/carbonStore/default/hive_carbon_example' ")

    stmt.execute("set hive.mapred.supports.subdirectories=true")
    stmt.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")

    val sql = "select id from hive_carbon_example"

    val res: ResultSet = stmt.executeQuery(sql)
    if (res.next) {
      val result = res.getString("id")
      System.out.println("+---+")
      System.out.println("| id|")
      System.out.println("+---+")
      System.out.println(s"| $result |")
      System.out.println("+---+")

    }

    hiveLocalServer2.stop(true)
    hiveLocalMetaStore.stop(true)
  }
}

// scalastyle:on println
