# Databricks notebook source exported at Tue, 29 Sep 2015 20:13:26 UTC
# MAGIC %md
# MAGIC ## Solutions

# COMMAND ----------

# MAGIC %md ### Assignment 1

# COMMAND ----------

sqlContext.sql("SELECT COUNT(DISTINCT(firstName)) AS count FROM names_by_year WHERE gender = 'M' AND year = 1938").show()

# COMMAND ----------

# MAGIC %md ### Assignment 2

# COMMAND ----------

sqlContext.sql("SELECT firstName, total FROM names_by_year WHERE gender = 'F' AND year = 1985 ORDER BY total, firstName LIMIT 5").show()

# COMMAND ----------

# MAGIC %md ### Assignment 3

# COMMAND ----------

def top_female_names_for_year(year, n, df):
  return sqlContext.sql("""SELECT firstName, total FROM names_by_year 
                           WHERE year = {0} AND gender = 'F'
                           ORDER BY total DESC LIMIT {1}""".format(year, n))
