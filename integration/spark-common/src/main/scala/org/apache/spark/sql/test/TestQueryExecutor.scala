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

package org.apache.spark.sql.test

import java.io.{File, FileInputStream, InputStream}
import java.util.ServiceLoader

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 * the sql executor of spark-common-test
 */
trait TestQueryExecutorRegister {
  def sql(sqlText: String): DataFrame

  def sqlContext: SQLContext
}

object TestQueryExecutor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val filesystem: InputStream = new FileInputStream(new File("target/classes/app.properties"))
  val properties = new java.util.Properties()
  properties.load(filesystem)
  val path: String = new File (properties.getProperty("file-source")+"../../../../..").getCanonicalPath

  println("\n\n ======$$====== path : "+path +"=="+properties.getProperty("type")+"======== \n")

  val projectPath = new File(this.getClass.getResource("/").getPath + "../../../..")
    .getCanonicalPath
  LOGGER.info(s"project path: $projectPath")
  val integrationPath = s"$projectPath/integration"
  val resourcesPath = "hdfs://192.168.2.142:54311"
  val storeLocation = "hdfs://192.168.2.142:54311/"
  val warehouse = s"$integrationPath/spark-common/target/warehouse"
  val metastoredb = s"$integrationPath/spark-common/target/metastore_db"
  val kettleHome = "/usr/local/spark-1.6.2/carbonlib/carbonplugins"
  val timestampFormat = "dd-MM-yyyy"

  val INSTANCE = lookupQueryExecutor.newInstance().asInstanceOf[TestQueryExecutorRegister]

  private def lookupQueryExecutor: Class[_] = {
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .iterator().next().getClass
  }

}
