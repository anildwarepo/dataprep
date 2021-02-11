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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex

class DataPrep() {

    @throws(classOf[IllegalArgumentException])
    def Start(infilePath: String, outfilePath: String, spark: SparkSession) : String  = {
         //val spark = SparkSession.builder.getOrCreate

        val versionInfo = "DataPrep 1.0.0"
        println(versionInfo)
        
        //val factsaleDF = spark.read.format("csv").option("header","true").option("delimiter","|").option("numPartitions", 3).load(infilePath)
        val factsaleDF = spark.read.format("parquet").option("numPartitions", 3).load(infilePath)
        var newfactsaleDF = factsaleDF
        for(col <- factsaleDF.columns){
            newfactsaleDF = newfactsaleDF.withColumnRenamed(col,col.replaceAll("\\s", ""))
        }

        newfactsaleDF = newfactsaleDF.select(col("*"), substring(col("Description"), 4, 6).as("Desc1"))

        val DecimalType = DataTypes.createDecimalType(18, 2)
        val typedDF = newfactsaleDF.withColumn("SaleKey",col("SaleKey").cast(LongType)).
        withColumn("CityKey",col("CityKey").cast(IntegerType)).
        withColumn("CustomerKey",col("CustomerKey").cast(IntegerType)).
        withColumn("BillToCustomerKey",col("BillToCustomerKey").cast(IntegerType)).
        withColumn("StockItemKey",col("StockItemKey").cast(IntegerType)).
        withColumn("InvoiceDateKey",col("InvoiceDateKey").cast(DateType)).
        withColumn("DeliveryDateKey",col("DeliveryDateKey").cast(DateType)).
        withColumn("SalespersonKey",col("SalespersonKey").cast(IntegerType)).
        withColumn("WWIInvoiceID",col("WWIInvoiceID").cast(IntegerType)).
        withColumn("Description",col("Description").cast(StringType)).
        withColumn("Package",col("Package").cast(StringType)).
        withColumn("Quantity",col("Quantity").cast(IntegerType)).
        withColumn("UnitPrice",col("UnitPrice").cast(DecimalType)).
        withColumn("TaxRate",col("TaxRate").cast(DecimalType)).
        withColumn("TotalExcludingTax",col("TotalExcludingTax").cast(DecimalType)).
        withColumn("TaxAmount",col("TaxAmount").cast(DecimalType)).
        withColumn("Profit",col("Profit").cast(DecimalType)).
        withColumn("TotalIncludingTax",col("TotalIncludingTax").cast(DecimalType)).
        withColumn("TotalDryItems",col("TotalDryItems").cast(IntegerType)).
        withColumn("TotalChillerItems",col("TotalChillerItems").cast(IntegerType)).
        withColumn("LineageKey",col("LineageKey").cast(IntegerType)).
        withColumn("Desc1",col("Desc1").cast(StringType))
        
        typedDF.write.mode("overwrite").option("numPartitions", 3).parquet(outfilePath)

        return "Completed"
    }

    
}