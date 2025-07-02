Part2 - Azure Healthcare RCM Data Engineering project

EMR - (Azure SQL DB)
- Patients
- Providers
- Transactions
- Departments
- Encounters

We already got the EMR data in bronze layer in parquet format 

-> Insurance provider will be placing the Claims data in landing zone
-> Pubilc apis for ICD and CPT codes placed in landing zone
landing -> flat files (csv) and ICD,CPT codes
bronze  -> parquet
silver  -> delta tables
gold    -> delta tables

**BRONZE LAYER**:

=> claims data from landing to bronze
=> NPI and ICD data (call api) and directly place in Bronze folder
=> CPT data from landing to bronze

**NOTE** : In Broze every thing we are keeping in parquet format

**BRONZE LAYER TO SILVER LAYER**:

=> clean the data
=> Common data module(CDM)- (For two databases hos a and hos b there will be different schema structures we will
get that into single schema using CDM)
=> Slowly Changing Dimenison(SCD2) : In Data Warehousing, Slowly Changing Dimensions (SCDs) handle how data changes over time in dimension tables. SCD Type 2 (SCD2) is the most commonly used type
when we want to preserve historical data.
=> We will keep the data in delta tables


**SILVER LAYER TO GOLD LAYER**:
=> Here we will create facts and dimensions

**Best Practices / Enhancements**:

=> Implement Key Vault
=> Improvise on naming convention
=> Make ADF pipeline parallel
=> is_active flag implement
=> Implement the unity catalog
=> adding retries

**Few things to note**:
1. All the data provided is generated using faker module
2. Data discrepancy and some joins might not work
3. Imporove the naming conventions / organize the code well

**Mounting ADLS storage account to Databricks file sysytem(DBFS)**:

1. unity catalog
2. catalog name -> Schema name (Database) -> Table name
3. dbutils.secrets.get('tt-hc-kv','tt-adls-access-key-dev')
 - **How to setup the Key Vault(KV)**:
 - tt-health-care-kv (azure portal)
 - create the secrets
   1. We have to create a scope in databricks using below
      https://adb-28...................16.azuredatabricks.net#secrets/createScope
   2. Manage Principal : Creator
   3. Write the below Azure Key Vault Options
      - DNS Name
      - Resource Id
  => If you want to access the key vault we have to create the app registrations as follows:
     1. Go to app registration and create a app for the service (ADF,ADB)
     3. ADF (Azure Data Factory)
     4. ADB (Azure Data Bricks)
     5. GO to the key vault -> Go to access policies
  
**Available Linked Services in Part 1**:
- Azure SQL DB
- ADLS Gen2
- Delta Lake

 Now we will create a new Key Vault(New one) linked service in part 2 and Databricks linked service(New one) also
 => Now previously we have hardcoded the secret values in linked service now we will remove them and replace with **Azure Key Vault**

 **How to implement active and inactive flag in config.csv**:

1. Go to azure data factory and launch studio
2. From config.csv if the is_active=0 do not execute but if it is 1 then execute the (if else pipeline with load as full or incremental disable the previous if else piepline and copy that in this)
3. And we have made the pipeline from s by removing the Autoincrement from the audit table and unchecking the Sequential variable in the pipeline
4. We created a linked service for azure key vault
5. We created one more linked service for databricks

**Step 1(Continuation) : Getting remaining datasets to Bronze layer**:

- claims and the NPI,ICD and CPT
- Claims and CPT (landing -> bronze)
- NPI and ICD codes (Pubilc api -> bronze)

**How to get Claims and CPT (landing -> bronze)**:
=> Go to the databricks folder and select the folder API extracts there will be two ipynb files to extract ICD and NPI codes see those files to mount the data to the bronze folder
=> NPI code starts with 1 but in our datasets its different
=> Data extracted from the API's is very less,so when you make join you may see lot of nulls

**How to place the claims data from landing -> bronze**:

=>  Generally the claims csv files are uploaded in the format of csv we have to place them in bronze folder
=> for the existing files we have added a new column data source by checking the hospital type and after that we place them in the bronze folder
=> we will create two separate folders in **Bronze** folder named **claims** and **cpt_codes** from the same claims csv files uploaded in the **landing** folder

**Now we have all the data in the Bronze folder i.e from API's,EMR and claims**

**Step2 : Bronze to silver**:
=> We will Full load the files for the fixed data like **Departments_F.py and Providers_F.py** that were there in the Silver folder
=> **EMR Data**:
  - Patients (SCD2)
  - Providers (full load i.e complete refresh)
  - Department (full load i.e complete refresh)
  - Transactions (SCD2)
  - Encounters (SCD2)
=> **Ex:** Providers_F.py

 from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/departments")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/departments")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

#Create the dept_id column and 
df_merged = df_merged.withColumn("SRC_Dept_id",f.col(""deptid") \
.withColumn("Dept_id",f.concat(f.col("deptid"),f.lit('-'),
f.col("datasource")))\
.drop("deptid")

- We will read the mounted data for departments and providers and we will create a Temporary view
- And then we will create the table by replacing some columns and insert the mounted department/Providers data into the newly created table

=> Now we will see what to do for changing data(**SCD2**) like **Patient.py etc..** (since patient data will be changed concurrently)
- For Patients we have to implement SCD2 and CDM(Common Data Module)
- If **is_quarantied=true means Bad Record**

  else **is_quarantied=false means Good Record**



 




