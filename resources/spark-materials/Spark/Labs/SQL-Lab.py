# Databricks notebook source exported at Tue, 29 Sep 2015 20:12:04 UTC
# MAGIC %md # DataFrames SQL Lab

# COMMAND ----------

# MAGIC %md In this lab, we're going to redo what we did in the previous DataFrames lab, except that we're going to use SQL, instead of API calls. We'll be using the same Parquet file.

# COMMAND ----------

# Spark 1.4
df = sqlContext.read.parquet("dbfs:/home/training/ssn/names.parquet")

# Spark 1.3
#df = sqlContext.parquetFile("dbfs:/home/training/ssn/names.parquet")

# COMMAND ----------

# MAGIC %md Once again, let's cache it.

# COMMAND ----------

ssnNames = df.cache()

# COMMAND ----------

# MAGIC %md The next step is to map the DataFrame to a temporary table that we can query.

# COMMAND ----------

ssnNames.registerTempTable("names_by_year")

# COMMAND ----------

# MAGIC %md Now we can query the table with SQL.

# COMMAND ----------

some_names = sqlContext.sql("SELECT firstName, total, year FROM names_by_year WHERE gender = 'M'")

# COMMAND ----------

# MAGIC %md We got back a DataFrame. Run the following command to verify:

# COMMAND ----------

some_names

# COMMAND ----------

some_names.count()

# COMMAND ----------

# MAGIC %md Note that RDD-style actions (like `take()` and `sample()`) return an array of `Row` objects for DataFrames and SQL.

# COMMAND ----------

some_names.take(5)

# COMMAND ----------

# sample 0.01% of the data, which ends up being between 80 and 90 rows
some_names.sample(False, 0.0001).show(30)

# COMMAND ----------

# MAGIC %md You can make the output even nicer by using `display()` helper function.

# COMMAND ----------

display(some_names.sample(False, 0.0001))

# COMMAND ----------

# MAGIC %md For SQL, the `%sql` Databricks Notebook command does the same thing:

# COMMAND ----------

# MAGIC %sql SELECT firstName, total, year FROM names_by_year LIMIT 20

# COMMAND ----------

# MAGIC %md Let's count the total number of items in the table, as we did with the API version of the lab.

# COMMAND ----------

sqlContext.sql("SELECT COUNT(firstName) FROM names_by_year").show()

# COMMAND ----------

# MAGIC %md As you might expect, the SQL `AS` clause works just fine:

# COMMAND ----------

sqlContext.sql("SELECT COUNT(firstName) AS count FROM names_by_year").show()

# COMMAND ----------

# MAGIC %md Let's figure out the number of distinct names in the table.

# COMMAND ----------

sqlContext.sql("SELECT COUNT(DISTINCT(firstName)) AS count FROM names_by_year").show()

# COMMAND ----------

# MAGIC %md ## Joins

# COMMAND ----------

# MAGIC %md SQL joins work, as well, including self-joins. The following SQL accomplishes the same job as the join we did in the API lab. Note that `LOWER` _does_ work here.

# COMMAND ----------

# MAGIC %sql SELECT n2010.firstName AS name, n1930.total AS total1930, n2010.total AS total2010
# MAGIC      FROM names_by_year n1930 INNER JOIN names_by_year n2010
# MAGIC        ON LOWER(n1930.firstName) = LOWER(n2010.firstName) AND n2010.gender = 'F' AND n1930.gender = 'F'
# MAGIC      WHERE n1930.year = 1930 AND n2010.year = 2010
# MAGIC      ORDER BY n2010.total DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md ## Note
# MAGIC 
# MAGIC 1. You don't have to capitalize the SQL keywords. I do it here merely for readability.

# COMMAND ----------

# MAGIC %md ## Assignment 1
# MAGIC 
# MAGIC Count the number of distinct boy's names for the year 1938.

# COMMAND ----------

# MAGIC %md ## Assignment 2
# MAGIC 
# MAGIC Find the five _least_ common female names from 1985, assuming an alphabetical sort. That is, find the female names with the smallest number of occurrences in 1985, sort them by name, and take the first five.

# COMMAND ----------

# MAGIC %md ## Assignment 3
# MAGIC 
# MAGIC Rewrite our `top_female_names_for_year` function, from the API lab, to use SQL, instead of the DataFrames API. Here are the instructions again:
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC In the cell below, you'll see an empty function, `top_female_names_for_year`. It takes three arguments:
# MAGIC 
# MAGIC * A year
# MAGIC * A number, _n_
# MAGIC * A `df` argument, which you should ignore. (It's only there so we can re-use the previous testers.)
# MAGIC 
# MAGIC It returns a new DataFrame that can be used to retrieve the top _n_ female names for that year (i.e., the _n_ names with the highest _total_ values).
# MAGIC 
# MAGIC Write that function. To test it, run the cell _following_ the function?i.e., the one containing the `%run` command. (This might take a few minutes.)

# COMMAND ----------

def top_female_names_for_year(year, n, df = None):
  return df.limit(n) # THIS IS NOT CORRECT! FIX IT!

# COMMAND ----------

# Transparent Tests
from test_helper import Test
def test_year(year, df):
    return [row.firstName for row in top_female_names_for_year(year, 5, df).collect()]

# COMMAND ----------

def run_tests():
  Test.assertEquals(test_year(1945, ssnNames), [u'Mary', u'Linda', u'Barbara', u'Patricia', u'Carol'], 'incorrect top 5 names for 1945')
  Test.assertEquals(test_year(1970, ssnNames), [u'Jennifer', u'Lisa', u'Kimberly', u'Michelle', u'Amy'], 'incorrect top 5 names for 1970')
  Test.assertEquals(test_year(1987, ssnNames), [u'Jessica', u'Ashley', u'Amanda', u'Jennifer', u'Sarah'], 'incorrect top 5 names for 1987')
  Test.assertTrue(len(test_year(1945, ssnNames)) <= 5, 'list not limited to 5 names')
  Test.assertTrue(u'James' not in test_year(1945, ssnNames), 'male names not filtered')
  Test.assertTrue(test_year(1945, ssnNames) != [u'Linda', u'Linda', u'Linda', u'Linda', u'Mary'], 'year not filtered')
  Test.assertEqualsHashed(test_year(1880, ssnNames), "2038e2c0bb0b741797a47837c0f94dbf24123447", "incorrect top 5 names for 1880")

run_tests()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Solution
# MAGIC 
# MAGIC If you're stuck, and you're really not sure how to proceed, feel free to check out the soultion. You'll find it in the same folder as the lab.

# COMMAND ----------


