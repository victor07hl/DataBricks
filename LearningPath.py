# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
df = spark.read.csv(path,header=True)


# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.select(F.sum("Delay"), F.avg("Delay"),
F.min("Delay"), F.max("Delay")).display()

# COMMAND ----------

# MAGIC %md #Creating Managed Tables

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
df = spark.read.csv(path,header=True)
df = df.toDF(*[col.replace(' ','') for col in df.columns])

df.write.format('delta').option('path','/mnt/bronze/mangedtbl_delta').saveAsTable('managedtbl_delta',mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables 

# COMMAND ----------

spark.sql(f"create table unmanaged_flight using csv location '{path}'")

# COMMAND ----------

# MAGIC %md #Viewing metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables 

# COMMAND ----------

# MAGIC %md #Reading images

# COMMAND ----------

# MAGIC %md ##Python

# COMMAND ----------

images_dir = '/databricks-datasets/learning-spark-v2/cctvVideos/train_images/'
images_df = spark.read.format('image').load(images_dir)

# COMMAND ----------

images_df.select('image.origin','image.width','image.nChannels','image.mode','image.data','label').limit(5).toPandas()#show(truncate=False)

# COMMAND ----------

Row = images_df.select('image.origin','image.width','image.nChannels','image.mode','image.data','label').where("origin like '%Browse2frame0000.jpg'").select('data').collect()

# COMMAND ----------

files = spark.read.format('binaryFile').option('pathGlobFilter','*.jpg').load(images_dir)

# COMMAND ----------

files.show(truncate=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/databricks-datasets/learning-spark-v2/cctvVideos/train_images/

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

# MAGIC %sh
# MAGIC dpkg-architecture

# COMMAND ----------

x = spark.conf.get('spark.storage.memoryFraction')

# COMMAND ----------

x

# COMMAND ----------

int(x[:-1])*0.6*0.0009765625,int(x[:-1])*0.4*0.0009765625

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')


# COMMAND ----------

spark.conf.get('spark.sql.partitions')

# COMMAND ----------

import pyspark.sql.functions as F
def pp(stri):
    return stri[0]
pp_udf = F.udf(pp)
df = spark.createDataFrame([(1,'vicot'),(2,'moreno')],['id','name'])
df.withColumn('third',pp_udf('name')).display()

# COMMAND ----------

# MAGIC %md #Functions

# COMMAND ----------

# MAGIC %md ##Isin

# COMMAND ----------

import  pyspark.sql.functions as F

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
df = spark.read.csv(path,header=True)

# COMMAND ----------

df.select('Incident Number','CallType').filter(F.col('CallType').isin(['Medical Incident','Structure Fire'])).display()

# COMMAND ----------

# MAGIC %md ##split

# COMMAND ----------

df = df.withColumn('split',F.split('Address',' '))
df.display()

# COMMAND ----------

# MAGIC %md ##concat_ws

# COMMAND ----------

df.withColumn('invsplit',F.concat_ws(',','split','rowid')).display()

# COMMAND ----------

# MAGIC %md ##count

# COMMAND ----------

df.groupBy('Unit ID').agg(F.count('Unit ID').alias('counter')).display()

# COMMAND ----------

# MAGIC %md #Repartition

# COMMAND ----------

df=df.repartition(120,['calltype','Call Final Disposition'] )#.count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md # CreateDataframe

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F


# COMMAND ----------

df = spark.createDataFrame([('red',),('blue',),('green',)],['color'])
df = df.withColumn('id',F.row_number().over(Window.partitionBy().orderBy('color')))
df.display()

# COMMAND ----------

df2 = spark.createDataFrame([(1,'azul'),(2,'cafe'),(3,'rojo')],['id','color'])


# COMMAND ----------

df.unionByName(df2).display()

# COMMAND ----------

df2.first().id

# COMMAND ----------

df2.take(1)

# COMMAND ----------


