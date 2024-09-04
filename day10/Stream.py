# Databricks notebook source
# MAGIC %sql
# MAGIC Batch
# MAGIC df=spark.read.csv("path")
# MAGIC df.write.mode("append").saveAsTable("tblname")
# MAGIC
# MAGIC Spark Structured Streaming
# MAGIC Gruanteen exactly read once
# MAGIC
# MAGIC df=spark.readStream.csv("path")
# MAGIC df.writeStream.table("tblname")    
# MAGIC
# MAGIC
# MAGIC Autoloader

# COMMAND ----------

# MAGIC %run /Workspace/Users/naval_1724213487060@npupgradassessment.onmicrosoft.com/day8/includes

# COMMAND ----------

users_schema="Id int,Name string, Gender string ,Salary int,Country string, Date string"

# COMMAND ----------

df=spark.readStream.csv(f"{input_path}streaminput",header=True)

# COMMAND ----------

df=spark.readStream.schema(users_schema).csv(f"{input_path}streaminput",header=True)

# COMMAND ----------

input_path


# COMMAND ----------

# DBTITLE 1,EL
spark\
.readStream\
.schema(users_schema)\
.csv(f"{input_path}streaminput",header=True)\
.writeStream\
.option("checkpointLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/checkpoint")\
.trigger(once=True)\
.table("stream")

# COMMAND ----------

df.writeStream.option("checkpointLocation","dbfs:/mnt/upgradtrendenceadls/delta/files/output/checkpoint").trigger(once=True).table("stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream

# COMMAND ----------

df.display()

# COMMAND ----------


