# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM prod.command_runs
# MAGIC WHERE date > date_sub(current_date, 7)
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT commandLanguage, notebookLanguage, count(*) AS num_command
# MAGIC FROM prod.command_runs
# MAGIC WHERE date > '2018-06-01'
# MAGIC GROUP BY commandLanguage, notebookLanguage
# MAGIC ORDER BY num_command DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod_ds.usage_logs WHERE metric = 'notebook' AND date = '2018-09-14' LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct shardName FROM prod_ds.usage_logs WHERE metric = 'notebook' AND date = '2018-09-14';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.eventType, tags.eventName, COUNT(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user FROM prod_ds.usage_logs WHERE metric = 'notebook' AND date = '2018-09-14'  GROUP BY tags.eventType, tags.eventName ORDER BY num_occurrence DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.eventType, tags.eventName, shardName, COUNT(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user FROM prod_ds.usage_logs WHERE metric = 'notebook' AND date = '2018-09-14' AND tags.eventType = 'autoComplete' GROUP BY tags.eventType, tags.eventName, shardName ORDER BY num_occurrence DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.eventType, tags.eventName, COUNT(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user FROM prod_ds.usage_logs WHERE metric = 'clientsideEvent' AND date = '2018-09-14'  GROUP BY tags.eventType, tags.eventName ORDER BY num_occurrence DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.eventType, tags.eventName, COUNT(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user FROM prod_ds.usage_logs WHERE metric = 'clientsideEvent' AND date = '2018-09-14' AND tags.eventName LIKE 'createTable%' GROUP BY tags.eventType, tags.eventName ORDER BY num_occurrence DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct metric
# MAGIC FROM prod_ds.usage_logs WHERE date = '2018-09-14';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod_ds.usage_logs WHERE date = '2018-09-14' AND metric = 'dataExported' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT shardName, count(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user
# MAGIC FROM prod_ds.usage_logs WHERE date = '2018-09-14' AND metric = 'dataExported' 
# MAGIC GROUP BY shardName
# MAGIC ORDER BY num_occurrence DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.notebookLanguage, count(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user
# MAGIC FROM prod_ds.usage_logs WHERE date = '2018-09-14' AND metric = 'dataExported' 
# MAGIC GROUP BY tags.notebookLanguage
# MAGIC ORDER BY num_occurrence DESC;

# COMMAND ----------

# Notebook language by customer

# COMMAND ----------

a = spark.sql("""
SELECT tags.notebookLanguage, count(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user FROM prod_ds.usage_logs WHERE date = '2018-09-14' AND metric = 'dataExported' GROUP BY tags.notebookLanguage ORDER BY num_occurrence DESC""")

# COMMAND ----------

import csv

# COMMAND ----------

str(a)

# COMMAND ----------

a.write.csv("test 10/10.1.csv")

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %sh ls /dbfs/"test 10"

# COMMAND ----------

!echo "asdf" > "/dbfs/test 10/10.1.csv"

# COMMAND ----------

from IPython.display import HTML
HTML("""
<a href="https://logfood.cloud.databricks.com/files/dbfs/test 10/10.1.csv" target="_blank"> link </a>
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tags.notebookLanguage, count(*) AS num_occurrence, COUNT(distinct tags.pseudoUserId) AS num_user
# MAGIC FROM prod_ds.usage_logs WHERE date = '2018-09-14' AND metric = 'dataExported' 
# MAGIC GROUP BY tags.notebookLanguage
# MAGIC ORDER BY num_occurrence DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.user_identity_lookup_2018_q4
# MAGIC WHERE pseudoUserId IS NOT null
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod_ds.usage_logs WHERE date = '2018-10-10' LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM prod.users_history WHERE user LIKE 'xhao@cppib.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.*
# MAGIC FROM prod.user_identity_lookup_2018_q4 a
# MAGIC JOIN
# MAGIC (SELECT distinct pseudoUserId
# MAGIC FROM prod.command_runs
# MAGIC WHERE date = '2018-11-01')  b ON a.pseudoUserId = b.pseudoUserId
# MAGIC ;

# COMMAND ----------

fdsf
  da