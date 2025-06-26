### Databricks Utilities : 

 

1. **dbutils.fs()** :

   Used to get all the files in a container in storage account 

Ex : dbutils.fs.ls(“abfss://<container_name>@<storage_account_name>.dfs.core.window.net/)


2. **dbutils.widgets** : 

dbutils.widgets is part of Databricks Utilities that lets you create interactive input widgets (like dropdowns, text boxes, and multi-select) directly in Databricks notebooks. 

These widgets allow you to pass parameters to your notebooks—which is especially useful in: 

Parameterized notebook workflows 

Job executions 

ETL pipelines 

Ex:  

- dbutils.widgets.text(“p_name”,”divya”)  -> This will create a text box in notebook 

- dbutils.widgets.get(“p_name”) -> To get the value of the variable 

3. **dbutils.secrets**: 

When working with secrets we use this 

Azure Keyvault : To store secrets  

Ex : dbutils.secrets.list(scope=’scope_name’) 
