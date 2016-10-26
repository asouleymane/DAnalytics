Good Afternoon,

I have this stack mostly setup. I have exhausted my knowledge and need a 2nd set of eyes to look at the link between livy and sparkmagic.

http://livy.io/

https://github.com/jupyter-incubator/sparkmagic

The EMR Cluster is working as it should. Running PySpark it's possible to import the reviews dataset and it creates the database that is parsible with SQL queries via the code below.

> ssh -i "/dsa/home/gogginss/goggins.pem" hadoop@ec2-54-67-30-210.us-west-1.compute.amazonaws.com

> pyspark

 >> from pyspark.sql import SparkSession

 >> spark = SparkSession \

 >>    .builder \

 >>    .appName("Python Spark SQL basic example") \

 >>    .config("spark.some.config.option", "some-value") \

 >>    .getOrCreate()

 > df = spark.read.json("In/reviews.json")



Spark 2.0

[http://spark.apache.org/docs/latest/sql-programming-guide.html](SPARK SQL Programming Guide)

Using Python version 2.7.10 (default, Jul 20 2016 20:53:27)
SparkSession available as 'spark'.
>>> from pyspark.sql import SparkSession
>>>
>>> spark = SparkSession \
...     .builder \
...     .appName("Python Spark SQL basic example") \
...     .config("spark.some.config.option", "some-value") \
...     .getOrCreate()
>>> df = spark.read.json("In/reviews.json")
>>> df.printSchema()
root
 |-- helpfuless_count: long (nullable = true)
 |-- helpfuless_score: long (nullable = true)
 |-- price: string (nullable = true)
 |-- productId: string (nullable = true)
 |-- profileName: string (nullable = true)
 |-- score: string (nullable = true)
 |-- summary: string (nullable = true)
 |-- text: string (nullable = true)
 |-- time: string (nullable = true)
 |-- title: string (nullable = true)
 |-- userId: string (nullable = true)

>>> df.createOrReplaceTempView("title")
>>> sqlDF = spark.sql("SELECT * FROM title LIMIT 10")
>>> sqlDF.show()
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
|helpfuless_count|helpfuless_score|   price|  productId|         profileName|score|             summary|                text|       time|               title|         userId|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
|               7|               7| unknown| B000179R3I| Jeanmarie Kabala...|  4.0| Periwinkle Dartm...| I own the Austin...| 1182816000|          Amazon.com| A3Q0VJTUO4EZ56|
|               0|               0|   17.99| B000GKXY34|          M. Gingras|  5.0|          Great fun!| Got these last C...| 1262304000| Nun Chuck, Novel...|  ADX8VLDUOL7BG|
|               1|               0|   17.99| B000GKXY34|     Maria Carpenter|  3.0|  more like funchuck| Gave this to my ...| 1224633600| Nun Chuck, Novel...| A3NM6P6BIWTIAE|
|               7|               7| unknown| 1882931173| Jim of Oz "jim-o...|  4.0| Nice collection ...| This is only for...|  940636800| Its Only Art If ...|  AVCGYZL8FQQTD|
|               4|               3|   15.99| B00002066I|             unknown|  5.0|           Inspiring| I hope a lot of ...|  939772800|                  ah|        unknown|
|               0|               0|   15.99| B00002066I|   Stephen McClaning|  5.0|            Great CD| My lovely Pat ha...| 1332288000|                  ah| A2KLYVAS0MIBMQ|
|               1|               1| unknown| B000058A81|            A reader|  5.0| First album I've...| We've come a lon...| 1096934400|        Chrono Cross| A18C9SNLZWVBIE|
|               1|               1| unknown| B000058A81|  Christopher Walden|  5.0| Pleasant to the ...| Final fantasy fa...| 1088121600|        Chrono Cross| A38QSOKE2DD8JD|
|               1|               1| unknown| B000058A81|             IcemanJ|  5.0| Much more than a...| This has got to ...| 1075939200|        Chrono Cross|  AKZLIIH3AP4RU|
|               1|               1| unknown| B000058A81|              Shadow|  5.0|            Amazing!| I used to find m...| 1035417600|        Chrono Cross| A1FELZOGR5DEOM|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+


On dev.dsa.missouri.edu there are 2 new kernels.

One is PySpark which is what will be used for our purposes.

The other is spark which should the the scala CLI.

The hello worlds I did came from the following guide:
http://spark.apache.org/docs/latest/sql-programming-guide.html

The amazon reviews are loaded in the hadoop file system:

[hadoop@ip-172-31-29-80 ~]$ /usr/bin/hadoop fs -ls In
Found 1 items
-rw-r--r--   1 hadoop hadoop 31374822340 2016-10-17 19:46 In/reviews.json



To connect to our aws instance do the following from dev.dsa.missouri.edu:

ssh -i "/dsa/home/gogginss/goggins.pem" hadoop@ec2-54-67-30-210.us-west-1.compute.amazonaws.com
ssh -i sijae.pem hadoop@ec2-54-67-30-210.us-west-1.compute.amazonaws.com


to start the livy server on the hadoop cluster run the following:

[hadoop@ip-172-31-29-80 ~]$ /home/hadoop/livy-server-0.2.0/bin/livy-server
16/10/18 20:11:47 INFO LivyServer: Using spark-submit version 2.0.0
16/10/18 20:11:48 WARN RequestLogHandler: !RequestLog
16/10/18 20:11:48 INFO WebServer: Starting server on http://ip-172-31-29-80.us-west-1.compute.internal:8998

This is required to be running for the kernel in jupyterhub to connect to the spark cluster.

This is mostly working however livy is throwing random exceptions and I have ran out of time to troubleshoot it for the moment.
