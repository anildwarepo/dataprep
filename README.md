# Data prep with Spark on AKS and Azure Synapse SQL Pool
This repo shows some of the fast data prep/data processing and data loading using Spark 3.0 on AKS and Azure Synapse SQL Pool. The goal being maximizing parallel data extraction, data processing and data loading. 
This repo has two projects
- SparkDataPrep

This is a Data Prep job that is used to process data stored on Azure Data Lake Storage Gen2. For more details on how to setup Spark 3.0 on AKS, please refer to [this repo](https://github.com/anildwarepo/spark-on-aks).

- AzureFunctionsOrchestrator

This is an orchestrator function that triggers
- data extraction from SQL Pool
- data processing using Spark Livy endpoint
- Data Loading into SQL Pool.

## Pre-requisites
- Spark configured on AKS. Please refer to [this repo](https://github.com/anildwarepo/spark-on-aks).
- Livy running on AKS. Please refer to [this repo](https://github.com/anildwarepo/spark-on-aks).
- Azure Synapse SQL Pool dedicated pool provisioned
- SQL User for SQL Pool configured with LargeRC resource class. 
- A data table loaded into Azure Synapse SQL Pool. 
- DATA_SOURCE and FILE_FORMAT configured in SQL pool pointing to Azure Data Lake Gen 2 as shown below.
- VS Code for building jar, running Azure Function.
- Docker for building docker containers. 
- java runtime for building jar. 

        CREATE EXTERNAL DATA SOURCE WWIStorage
        WITH
        (
            TYPE = Hadoop,
            LOCATION = 'abfss://<container>@<adlsgen2>.dfs.core.windows.net'
        );

        CREATE EXTERNAL FILE FORMAT snappyparquetfileformat  
        WITH (  
            FORMAT_TYPE = PARQUET,  
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
        );  

## Steps to Run

### Step 1
Define External Data Source and FILE FORMAT in SQL Pool

### Step 2
Prepare Spark and Livy containers and push them to container registry. 
Run livy server on AKS as shown in [this repo](https://github.com/anildwarepo/spark-on-aks).

### Step 3
Prepare jar file by compiling SparkDataPrep in this repo. Update column names based on the table in SQL Pool and data prep scenario. In this repo, the 'Description' column is split into a new column. Build jar using:

    mvn clean package

This creates an uber jar with all dependencies.
Upload jar to Azure Data Lake Gen2 container. 

### Step 4
Update Storage Connection string, Storage Access Key, SQL Pool name and credentials in Azure Functions Orchestrator.
Run the function locally and trigger function using HTTP endpoint. 