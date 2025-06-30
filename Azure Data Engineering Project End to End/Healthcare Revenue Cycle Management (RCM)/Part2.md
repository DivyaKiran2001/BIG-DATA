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




