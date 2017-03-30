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
import java.sql.DriverManager

// Write carbondata file by spark and read it by flink
// scalastyle:off println
object HiveExample {
  val currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath

  def main(args: Array[String]): Unit = {

    val hiveLocalMetaStore = HiveLocalMetaStore.setup(currentPath)
    println("\n========================successfully Started metastore")
    val hiveLocalServer2 = HiveLocalServer2.setup(currentPath)
    println("\n========================successfully Started HiveServer")

    Thread.sleep(1000)
    println("starting client")
    Class.forName("org.apache.hive.jdbc.HiveDriver");

    val con = DriverManager.getConnection("jdbc:hive2:/jdbc:hive2://localhost:10001/testHiveDb",
      "",
      "");

    // Create the DB
    try {
      val createDbDdl = "CREATE DATABASE IF NOT EXISTS testHiveDb";

      val stmt = con.createStatement();
      stmt.execute(createDbDdl);
    } catch {
      case e: Exception => e.printStackTrace();
    }

    // Drop the table incase it still exists
    val dropDdl = "DROP TABLE if exists testHiveDb.testHiveTb ";
    val stmt1 = con.createStatement();
    stmt1.execute(dropDdl);

    // Create the ORC table
    val createDdl = "CREATE TABLE IF NOT EXISTS testHiveDb.testHiveTb PARTITIONED BY (dt STRING) " +
                    "CLUSTERED BY (id) INTO 16 BUCKETS " +
                    "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
    val stmt2 = con.createStatement();
    stmt2.execute(createDdl);

    // Issue a describe on the new table and display the output
    val resultSet = stmt2.executeQuery("DESCRIBE FORMATTED testHiveDb.testHiveTb");
    while (resultSet.next()) {
      var i = 0;
      do {
        System.out.print(resultSet.getString(i));
        i = i + 1
      }
      while (resultSet.getString(i) != null)

      System.out.println();
    }

    hiveLocalServer2.stop(true)
    hiveLocalMetaStore.stop(true)
  }
}

// scalastyle:on println
