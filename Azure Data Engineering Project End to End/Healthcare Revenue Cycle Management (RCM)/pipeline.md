### Pipeline : 

Pipeline is made up of activities

- **Look Up activity** :

Will read the config file (i.e config container) (Used to read a file or a table)

**Steps**:

1. First read the config file (i.e Look Up activity)

2. Iterate over each entity one by one in that config file using foreach and get metadata using MEtadata activity

3. If the file exists (ex : encounters.parquet file) is present in bronze folder then we move into the archive folder present in bronze and do the remaining steps 

**source**:

container - bronze
file_path - hosa
file_name - encounters

**target**:

container - bronze
file_path - hosa / archive / year / month / day
file_name - encounters

4. Take the data from Azure SQL DB and put it in Bronze container

source - Azure sql DB
target - parquet generic

6. If its a full load or incremental if load type is full then we will take all the data from sql db (ex: encounters table) and put it in parquet file

source - Azure SQL DB
(Database name,Schema name and Table name)

7. If load type is incremental then we will have a LOok Up activity do the below steps

- check the audit table 
- and the get date of last time it is loaded and we take all the data on and after that data and update the parquet file  and then we finally update the audit table

8. Finally run the pipeline to see the changes in the audit table and see if the sql data is transfered from the Azure SQL DB to Bronze layer in containers of Azure storage account


  
