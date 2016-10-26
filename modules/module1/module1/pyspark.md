All,


I managed to get this running today. There is still an extreme disconnect between this and Jupyterhub, however this will allow for dev work to start. At some point we should meet so I can hand out keys and such.

 > ssh -i "/dsa/home/gogginss/goggins.pem" hadoop@ec2-54-67-30-210.us-west-1.compute.amazonaws.com

Python 2.7.10 (default, Jul 20 2016, 20:53:27)
[GCC 4.8.3 20140911 (Red Hat 4.8.3-9)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel).
16/10/17 19:50:54 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.

Welcome to


Using Python version 2.7.10 (default, Jul 20 2016 20:53:27)
SparkSession available as 'spark'.
 > from pyspark.sql import SparkSession
 >
 > spark = SparkSession \
... .builder \
... .appName("Python Spark SQL basic example") \
... .config("spark.some.config.option", "some-value") \
... .getOrCreate()
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

>>> df.show()
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
|helpfuless_count|helpfuless_score| price| productId| profileName|score| summary| text| time| title| userId|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
| 7| 7| unknown| B000179R3I| Jeanmarie Kabala...| 4.0| Periwinkle Dartm...| I own the Austin...| 1182816000| Amazon.com| A3Q0VJTUO4EZ56|
| 0| 0| 17.99| B000GKXY34| M. Gingras| 5.0| Great fun!| Got these last C...| 1262304000| Nun Chuck, Novel...| ADX8VLDUOL7BG|
| 1| 0| 17.99| B000GKXY34| Maria Carpenter| 3.0| more like funchuck| Gave this to my ...| 1224633600| Nun Chuck, Novel...| A3NM6P6BIWTIAE|
| 7| 7| unknown| 1882931173| Jim of Oz "jim-o...| 4.0| Nice collection ...| This is only for...| 940636800| Its Only Art If ...| AVCGYZL8FQQTD|
| 4| 3| 15.99| B00002066I| unknown| 5.0| Inspiring| I hope a lot of ...| 939772800| ah| unknown|
| 0| 0| 15.99| B00002066I| Stephen McClaning| 5.0| Great CD| My lovely Pat ha...| 1332288000| ah| A2KLYVAS0MIBMQ|
| 1| 1| unknown| B000058A81| A reader| 5.0| First album I've...| We've come a lon...| 1096934400| Chrono Cross| A18C9SNLZWVBIE|
| 1| 1| unknown| B000058A81| Christopher Walden| 5.0| Pleasant to the ...| Final fantasy fa...| 1088121600| Chrono Cross| A38QSOKE2DD8JD|
| 1| 1| unknown| B000058A81| IcemanJ| 5.0| Much more than a...| This has got to ...| 1075939200| Chrono Cross| AKZLIIH3AP4RU|
| 1| 1| unknown| B000058A81| Shadow| 5.0| Amazing!| I used to find m...| 1035417600| Chrono Cross| A1FELZOGR5DEOM|
| 1| 1| unknown| B000058A81| Eric N. Gross| 5.0| If you want RPG ...| I'm not an exper...| 1019779200| Chrono Cross| A2KACA2XFCGS8S|
| 1| 1| unknown| B000058A81| Sam S.| 5.0| An amazing OST!!...| I played Chrono ...| 999820800| Chrono Cross| AR66260V9H91B|
| 3| 2| unknown| B000058A81| highter| 4.0| Stuning even for...| This sound track...| 985392000| Chrono Cross| A3CKAD2MAEM157|
| 0| 0| unknown| B000058A81| Zar| 5.0| Crono cross cd| Love the cd's an...| 1342310400| Chrono Cross| A345GZYTC6CQ6Z|
| 0| 0| unknown| B000058A81| John P. Murphy "...| 5.0| Great music to a...| Attn| 1320364800| Chrono Cross| A1QG359N5QLUZK|
| 0| 0| unknown| B000058A81| Tein N.| 5.0| The best soundtr...| I'm reading a lo...| 1209945600| Chrono Cross| A2J7Q6SC7ZG8I6|
| 0| 0| unknown| B000058A81| Carmen "Lu"| 5.0| HURRAY, Chrono C...| When i played th...| 995760000| Chrono Cross| A10B5XF3C6UFOV|
| 0| 0| unknown| B000058A81| Sakkano Imako| 5.0| Even for a non-p...| Chrono Cross is ...| 986342400| Chrono Cross| A8810FHPNEREL|
| 1| 0| unknown| B000058A81| unknown| 5.0| Best Damn CD Eve...| This CD has touc...| 1083196800| Chrono Cross| unknown|
| 1| 0| unknown| B000058A81| unknown| 5.0| Nobou can't compare| As a frequent pl...| 1046304000| Chrono Cross| unknown|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
only showing top 20 rows

>>> df.createOrReplaceTempView("title")
>>> sqlDF = spark.sql("SELECT * FROM title LIMIT 10")
>>> sqlDF.show()
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
|helpfuless_count|helpfuless_score| price| productId| profileName|score| summary| text| time| title| userId|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+
| 7| 7| unknown| B000179R3I| Jeanmarie Kabala...| 4.0| Periwinkle Dartm...| I own the Austin...| 1182816000| Amazon.com| A3Q0VJTUO4EZ56|
| 0| 0| 17.99| B000GKXY34| M. Gingras| 5.0| Great fun!| Got these last C...| 1262304000| Nun Chuck, Novel...| ADX8VLDUOL7BG|
| 1| 0| 17.99| B000GKXY34| Maria Carpenter| 3.0| more like funchuck| Gave this to my ...| 1224633600| Nun Chuck, Novel...| A3NM6P6BIWTIAE|
| 7| 7| unknown| 1882931173| Jim of Oz "jim-o...| 4.0| Nice collection ...| This is only for...| 940636800| Its Only Art If ...| AVCGYZL8FQQTD|
| 4| 3| 15.99| B00002066I| unknown| 5.0| Inspiring| I hope a lot of ...| 939772800| ah| unknown|
| 0| 0| 15.99| B00002066I| Stephen McClaning| 5.0| Great CD| My lovely Pat ha...| 1332288000| ah| A2KLYVAS0MIBMQ|
| 1| 1| unknown| B000058A81| A reader| 5.0| First album I've...| We've come a lon...| 1096934400| Chrono Cross| A18C9SNLZWVBIE|
| 1| 1| unknown| B000058A81| Christopher Walden| 5.0| Pleasant to the ...| Final fantasy fa...| 1088121600| Chrono Cross| A38QSOKE2DD8JD|
| 1| 1| unknown| B000058A81| IcemanJ| 5.0| Much more than a...| This has got to ...| 1075939200| Chrono Cross| AKZLIIH3AP4RU|
| 1| 1| unknown| B000058A81| Shadow| 5.0| Amazing!| I used to find m...| 1035417600| Chrono Cross| A1FELZOGR5DEOM|
+----------------+----------------+--------+-----------+--------------------+-----+--------------------+--------------------+-----------+--------------------+---------------+

>>>
