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
package com.anildwa

import org.apache.spark.sql._

/**
 * Dataprep
 * 
 * @author Anil Dwarakanath
 */
object DataPrepMain {
  
/**
  * Needs these arguments
  * 
  * @arg(0) filepath file path for stream abfss://container@storage.dfs.core.windows.net or /mnt/filepath
  * @arg(1) fileformat delta or parquet, csv etc 
  * @arg(2) eventhub connection string Endpoint=sb://<eventhubname>.servicebus.windows.net/;SharedAccessKeyName=<SharedAccessKeyName>;SharedAccessKey=<shared access key secret>
  * @arg(3) eventhub topic name
  */
  @throws(classOf[IllegalArgumentException])
  def main(args : Array[String]) {
   
    
    if (args.length < 2) {
        throw new IllegalArgumentException("FilePaths not found in Argument")
    }
     val spark = SparkSession
      .builder
      .appName("DataPrep")
      .getOrCreate()
    //Create instance of EventHubListener
    var dataPrep = new DataPrep()
    dataPrep.Start(args(0),args(1), spark)
    
    spark.stop()
  }

}
