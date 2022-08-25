# Databricks notebook source
dbutils.fs.ls('/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/')

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/')

# COMMAND ----------

pathList = [x.path for x in files]

# COMMAND ----------

sizeList = [x.size for x in files]

# COMMAND ----------

filesRDD = sc.parallelize(pathList)

# COMMAND ----------

filesRDD.count()

# COMMAND ----------

filesRDD.collect()

# COMMAND ----------

csvFilesRDD = filesRDD.filter(lambda y: y.endswith('.csv'))

# COMMAND ----------

csvFilesRDD.collect()

# COMMAND ----------

import datetime
datetime.datetime.strptime("04/12/2022", "%m/%d/%Y").strftime("%Y%m%d")

# COMMAND ----------

import re
#Assumes input MM-DD-YYYY .csv
def convert_name(filepath):
    m = re.match ("^.*([0-9]{2})-([0-9]{2})-([0-9]{4}).csv$", filepath)
    return int(m.group(3) + m.group(1) + m.group(2))

# COMMAND ----------

csvPairRDD = csvFilesRDD.map(lambda y: (convert_name(y), y))
csvPairRDD.take(5)

# COMMAND ----------

convert_name(csvFilesRDD.collect()[1])

# COMMAND ----------

sortRDD = csvPairRDD.sortByKey(False)
sortRDD.take(5)

# COMMAND ----------

chosenfile = sortRDD.map(lambda line: line[1])
chosenfile.take(2)

# COMMAND ----------

chosenfile = 'dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/03-11-2021.csv'

# COMMAND ----------

chosenFileDF = spark.read.option("header", True).csv(chosenfile)

# COMMAND ----------

chosenFileDF.display()
