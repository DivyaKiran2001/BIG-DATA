               +-----------------------------+
               |       Delta Table           |
               | (stored in Parquet format)  |
               +-------------+---------------+
                             |
                             v
         +------------------------+   +----------------------+
         | _delta_log (JSON logs) | + | Parquet data files   |
         +------------------------+   +----------------------+
                             |
                             v
               Managed by Delta Engine (Databricks/Synapse)


Delta tables are stored as Parquet files with a special _delta_log/ folder.

Each transaction is recorded as a JSON file in _delta_log/.

### Delta Lake :

Delta Lake is an open-source storage layer that brings ACID transactions, schema enforcement, and time travel to Apache Spark and big data workloads. 
In Azure, it is deeply integrated with Azure Databricks and Azure Synapse Analytics.

**NOTE**:

### %run command
The %run command is a magic command in Databricks notebooks that lets you include and run another notebook inside the current notebook.

### Delta Tables :

**NOTE** : First you have to create database before creating delta tables
A Delta Table is a data table stored in Parquet format with a transaction log (_delta_log) that
enables database-like features in a data lake.

| Type                     | Description                                                                                           |
| ------------------------ | ----------------------------------------------------------------------------------------------------- |
| **Managed Delta Table**  | Both **data and metadata** are managed by Databricks. Data is stored in Databricks-managed paths.     |
| **External Delta Table** | Only the **metadata** is managed by Databricks. Data is stored in an external path (e.g., ADLS Gen2). |
