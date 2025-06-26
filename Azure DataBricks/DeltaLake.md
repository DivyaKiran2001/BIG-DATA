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

**Delta Tables Functionalities** :

1. **INSERT** :
   When an insert operation takes place in external delta table the below steps will happen :

| Step | Action                                                                                                                            |
|------|------------------------------------------------------------------------------------------------------------------------------------|
| 1️⃣  | The inserted row is written to a new **Parquet file** (e.g., `part-00000.snappy.parquet`) in the external path.                   |
| 2️⃣  | A new **JSON log file** (e.g., `00000000000000000001.json`) is created in `_delta_log/`, describing the newly added Parquet file. |
| 3️⃣  | The log file records details such as operation type (`add`), file path, size, schema, partition info, etc.                        |
| 4️⃣  | A **new table version** is now available.                                                                                          |
| 5️⃣  | If checkpointing is due (e.g., after 10 commits), a `.checkpoint.parquet` is also created.                                        |

2. **DELETE** :
   When you run a DELETE on a Delta Table (managed or external), Delta Lake does not immediately delete the physical data from storage. Instead, it uses a transaction log mechanism to log the removal of affected files and creates new files if needed.

**Tombstoning** :
Tombstoning is the process by which deleted or updated files are marked as removed in Delta Lake's transaction log — but not immediately deleted from storage.

**Data Versioning**:

Data versioning in Delta Lake refers to the ability to track, access, and restore previous versions of a Delta table using transaction logs stored in the _delta_log/ directory.

**TIME TRAVEL** :

We can go back to the prevoius versions of delta lake tables by using the **RESTORE** command as follows:

Ex : RESTORE TABLE salesdb.exttable TO VERSION AS OF 2

**NOTE** :

-> We cannot manually delete the parquet files which are present below the delta log folder it will break the whole table structure so instead of doing this we use VACCUM command

**VACCUM**:

The VACUUM command in Delta Lake is used to permanently delete old files (data files no longer needed) from the Delta table's storage to free up space.

Ex : VACCUM salesdb.exttable RETAIN 0 hrs

**Delta Table Optimization** :

1. **OPTIMIZE** : Small Partitions will be combined to a bigger partition
   
OPTIMIZE salesDB.exttable

2. **ZORDER BY**:

OPTIMIZE salesDB.exttable ZORDER BY (id)







   
