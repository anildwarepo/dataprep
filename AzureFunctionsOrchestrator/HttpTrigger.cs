using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace anildwa.Function
{
    public static class HttpTrigger
    {

        static string livyServerEndpoint = "";
        static string connectionString = "Server=tcp:<synapse-dedicated-sqlpoolname>.sql.azuresynapse.net,1433;Initial Catalog=SqlPool1;Persist Security Info=False;User ID=<userid>;Password=<Passowrd>;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
           
        [FunctionName("HttpTriggerCSharp")]
        public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            
            Wrap("ExportSQLPoolTable", ExportSQLPoolTable, log);
            Wrap("RunDataPrepSparkJob", RunDataPrepSparkJob, log);
            Wrap("ImportIntoSQLPoolTable", ImportIntoSQLPoolTable, log);
            Wrap("LoadDataIntoSQLPool", LoadDataIntoSQLPool, log);
           
            return new OkObjectResult($"DataPrepJobCompleted");
            
        }

        private static void Wrap(string actionName, Action<ILogger> action, ILogger log)
        {
            var watch = Stopwatch.StartNew();
            try
            {
                log.LogInformation($"Running {actionName}");
                action(log);
            }
            catch(Exception ex)
            {
                log.LogInformation($"Exception occurred in {actionName}: {ex.Message}");
            }
            finally
            {
                watch.Stop();
                log.LogInformation($"{actionName} executed in {watch.ElapsedMilliseconds} ms");
            }
        }
        

        private static void RunDataPrepSparkJob(ILogger log)
        {
            string requestUri = $"http://{livyServerEndpoint}:8998/batches";
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUri);
            request.Method = "POST";
            request.ContentType = "application/json; charset=UTF-8";
            string livyRequest = $@"{{
                ""name"": ""dataprep-{Guid.NewGuid().ToString()}"",
                ""className"": ""com.anildwa.DataPrepMain"",
                ""numExecutors"": 3,
                ""driverCores"": 1,
                ""driverMemory"": ""1g"",
                ""executorMemory"": ""1g"",
                ""executorCores"": 2,
                ""conf"": {{
                        ""spark.executor.instances"" : 3, 
                        ""spark.eventLog.enabled"" : ""true"", 
                        ""spark.eventLog.dir"" :""abfss://<container>@<azure data lake gen2 name>.dfs.core.windows.net/logs"", 
                        ""spark.sql.parquet.compression.codec"" : ""snappy"",
                        ""spark.kubernetes.namespace"" : ""spark"",  
                        ""spark.kubernetes.authenticate.driver.serviceAccountName"" : ""spark-sa"", 
                        ""spark.kubernetes.executor.podTemplateFile"" : ""/opt/livy/work-dir/executor-pod-template.yaml"",
                        ""spark.kubernetes.container.image"" : ""<azure container registry name>.azurecr.io/spark:3.0.1"",
                        ""spark.kubernetes.container.image.pullPolicy"" :""IfNotPresent"",
                        ""spark.hadoop.fs.azure.account.key.anildwastoragewestus2.dfs.core.windows.net"" : ""<ADLS Gen2 Account Access Key>""
                }},
                ""file"": ""abfss://jars@<azure data lake gen2 name>.dfs.core.windows.net/SparkDataPrep-1.0.0.jar"",
                ""args"": [""abfss://output@<azure data lake gen2 name>.dfs.core.windows.net/export"", ""abfss://output@<azure data lake gen2 name>.dfs.core.windows.net/dataprepped.parquet""]
                }}";

            using(Stream dataStream = request.GetRequestStream())
            {
                byte[] bytes = Encoding.ASCII.GetBytes(livyRequest);
                dataStream.Write(bytes, 0, bytes.Length);
            }

            using (HttpWebResponse resp = (HttpWebResponse)request.GetResponse())
            {
                try
                {
                    Stream receiveStream = resp.GetResponseStream();
                    Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
                    StreamReader readStream = new StreamReader( receiveStream, encode );
                    JObject livyResponse = JObject.Parse(readStream.ReadToEnd());
                    while(true)
                    {
                        if(PollLivy(livyResponse,log))
                        {
                            break;
                        }
                        
                        Task.Delay(5000).GetAwaiter().GetResult();
                        
                    }
                   
                }
                catch(Exception ex)
                {
                    log.LogInformation(ex.ToString());
                }
            }
        }
        private static bool PollLivy(JObject livyResponse, ILogger log)
        {
            
            string requestUri = $"http://{livyServerEndpoint}:8998/batches/{livyResponse.GetValue("id").ToString()}";

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUri);
            request.Method = "GET";
            request.ContentType = "application/json; charset=UTF-8";
            JObject pollResponse;
            using (HttpWebResponse resp = (HttpWebResponse)request.GetResponse())
            {
                Stream receiveStream = resp.GetResponseStream();
                Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
                StreamReader readStream = new StreamReader( receiveStream, encode );
                pollResponse = JObject.Parse(readStream.ReadToEnd());

                if(pollResponse.GetValue("state").ToString() == "running")
                {
                    log.LogInformation($"Spark Job State:{pollResponse.GetValue("state").ToString()}");
                    return false;
                }
                
            }
            log.LogInformation($"Spark Job State:{pollResponse.GetValue("state").ToString()}");
            return true;
        }

        private static void ImportIntoSQLPoolTable(ILogger log)
        {
            string commandCreateImportTable = @"IF
                    EXISTS (SELECT 1 FROM
                    sysobjects WHERE type = 'U' and name = 'ext_FactSaleFromParquetImport')
                    BEGIN
                    DROP EXTERNAL TABLE
                    [wwi].[ext_FactSaleFromParquetImport]
                    END
                    CREATE EXTERNAL TABLE [wwi].[ext_FactSaleFromParquetImport]
                    (
                        [SaleKey] [bigint]  NOT NULL,
                        [CityKey] [int] NOT NULL,
                        [CustomerKey] [int] NOT NULL,
                        [BillToCustomerKey] [int] NOT NULL,
                        [StockItemKey] [int] NOT NULL,
                        [InvoiceDateKey] [date] NOT NULL,
                        [DeliveryDateKey] [date] NULL,
                        [SalespersonKey] [int] NOT NULL,
                        [WWIInvoiceID] [int] NOT NULL,
                        [Description] [nvarchar](100) NOT NULL,
                        [Package] [nvarchar](50) NOT NULL,
                        [Quantity] [int] NOT NULL,
                        [UnitPrice] [decimal](18, 2) NOT NULL,
                        [TaxRate] [decimal](18, 3) NOT NULL,
                        [TotalExcludingTax] [decimal](18, 2) NOT NULL,
                        [TaxAmount] [decimal](18, 2) NOT NULL,
                        [Profit] [decimal](18, 2) NOT NULL,
                        [TotalIncludingTax] [decimal](18, 2) NOT NULL,
                        [TotalDryItems] [int] NOT NULL,
                        [TotalChillerItems] [int] NOT NULL,
                        [LineageKey] [int] NOT NULL,
                        [Desc1] [nvarchar](50) NOT NULL
                    )
                    WITH (LOCATION='/dataprepped.parquet/',
                        DATA_SOURCE = WWIStorage1,  
                        FILE_FORMAT = snappyparquetfileformat

                    )";
            ExecuteSQLCommand(commandCreateImportTable);
        }

        private static void ExportSQLPoolTable(ILogger log)
        {
            string commandTextCETAS = @"IF
                    EXISTS (SELECT 1 FROM
                    sysobjects WHERE type = 'U' and name = 'ext_FactSaleFromParquetExport')
                    BEGIN
                    DROP EXTERNAL TABLE
                    [wwi].[ext_FactSaleFromParquetExport]
                    END
                    CREATE EXTERNAL TABLE [wwi].[ext_FactSaleFromParquetExport]
                        WITH (
                            LOCATION = '/export/',  
                            DATA_SOURCE = WWIStorage1,  
                            FILE_FORMAT = snappyparquetfileformat  
                    )
                        AS SELECT * FROM [wwi].[FactSale]";
            ExecuteSQLCommand(commandTextCETAS);
        }

        private static void LoadDataIntoSQLPool(ILogger log) 
        {  
            string commandTextCTAS = @"IF
                    EXISTS (SELECT 1 FROM
                    sysobjects WHERE type = 'U' and name = 'FactSaleFromParquet1')
                    BEGIN                        
                    DROP TABLE
                    [wwi].[FactSaleFromParquet1]
                    END
                    CREATE TABLE [wwi].[FactSaleFromParquet1]
                    WITH
                    (
                        DISTRIBUTION = ROUND_ROBIN,
                        HEAP
                    )
                    AS
                    SELECT * FROM [wwi].[ext_FactSaleFromParquetImport]";
            ExecuteSQLCommand(commandTextCTAS);
        } 

        private static void ExecuteSQLCommand(string commandText)  
        {
            using (SqlConnection conn = new SqlConnection(connectionString)) 
            {  
                using (SqlCommand cmd = new SqlCommand(commandText, conn)) 
                {  
                    cmd.CommandType = CommandType.Text;  
                    cmd.CommandTimeout = 30000;
                    conn.Open();  
                    cmd.ExecuteScalar();  
                }  
            }  
        }
    }
}
