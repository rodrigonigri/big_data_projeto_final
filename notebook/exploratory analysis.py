# Databricks notebook source
import pyspark.sql.functions as func

# COMMAND ----------

df_mat = spark.read.csv("s3://bigdata-rais/projeto/rodrigong1/student-mat.csv",header=True,inferSchema=True)

df_port = spark.read.csv("s3://bigdata-rais/projeto/rodrigong1/student-por.csv",header=True,inferSchema=True)

# COMMAND ----------

df_port.limit(4).display()

# COMMAND ----------

df_mat.limit(4).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### No fim do projeto, vamos criar um modelo para prever a nota final baseado nos dados:
# MAGIC - sex
# MAGIC - age
# MAGIC - pstatus (T = together, A = apart)
# MAGIC - Medu
# MAGIC - Fedu
# MAGIC - Mjob
# MAGIC - Fjob
# MAGIC - traveltime (time to go to school; 1 = <15 min., 2 = 15-30 min., 3 = 30-60 min., 4 = >60 min.)
# MAGIC - studytime (weekly study time; 1 = <2 hours, 2 = 2-5 hours, 3 = 5-10 hours, 4 = >10 hours)
# MAGIC - failures 
# MAGIC - schoolsup (extra educational support)
# MAGIC - famsup 
# MAGIC - activities 
# MAGIC - higher (wants to take higher education)
# MAGIC - internet
# MAGIC - romantic 
# MAGIC - Dalc (daily alcohool consumption; numeric: from 1 - very low to 5 - very high)
# MAGIC - Walc (weekly alcohool consumption; numeric: from 1 - very low to 5 - very high)
# MAGIC - health (current health status; numeric: from 1 - very bad to 5 - very good)
# MAGIC - absences (number of school absences; numeric: from 0 to 93)
# MAGIC
# MAGIC - G1 (first period grade; numeric: from 0 to 20)
# MAGIC - G2 (second period grade; numeric: from 0 to 20)
# MAGIC - G3 (final grade; numeric: from 0 to 20, output target)
# MAGIC

# COMMAND ----------

# vamos juntar os dois dataframes em um só
df = df_mat.union(df_port)

# COMMAND ----------

print("{} linhas e {} colunas".format(df.count(), len(df.columns)))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select('age', 'failures').describe().show()

# COMMAND ----------


