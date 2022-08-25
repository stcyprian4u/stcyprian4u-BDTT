-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC import pyspark.sql.functions as A
-- MAGIC 
-- MAGIC clinicaltrial_2021 = spark.read.options(delimiter = "|", header = True, inferSchema = True).csv("/FileStore/tables/clinicaltrial_2021.csv")
-- MAGIC 
-- MAGIC pharma = spark.read.options(escape = "\"", header = True, inferSchema = True).csv("/FileStore/tables/pharma.csv")
-- MAGIC 
-- MAGIC mesh = spark.read.options(escape = "\"", header = True, inferSchema = True).csv("/FileStore/tables/mesh.csv").withColumn("code", A.split(A.col("tree"), "\.")[0]).withColumn("term", A.trim("term"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC clinicaltrial_2021.write.format("parquet").mode("overwrite").saveAsTable("clinicaltrial_2021")
-- MAGIC pharma.write.format("parquet").mode("overwrite").saveAsTable("pharma")
-- MAGIC mesh.write.format("parquet").mode("overwrite").saveAsTable("mesh")

-- COMMAND ----------

--#Q1 = "The number of studies in the dataset. You must ensure that you explicitly check distinct studies."

SELECT COUNT(DISTINCT(Id)) 
FROM clinicaltrial_2021
WHERE Id IS NOT NULL;

-- COMMAND ----------

--#Q2 = "You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent."

SELECT Type, COUNT(*) frequency
FROM clinicaltrial_2021
WHERE Type IS NOT NULL
GROUP BY Type
ORDER BY frequency DESC;

-- COMMAND ----------

--#Q3 = "The top 5 conditions (from Conditions) with their frequencies." 

SELECT condition, COUNT(*) frequency
FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) condition FROM clinicaltrial_2021
WHERE Conditions IS NOT NULL)
GROUP BY condition
ORDER BY frequency DESC
limit 5;

-- COMMAND ----------

--#Q4 = "Each condition can be mapped to one or more hierarchy codes. The client wishes to know the 5 most frequent roots (i.e. the sequence of letters and numbers before the first full stop) after this is done."

WITH y AS (
SELECT condition, COUNT(*) frequency
FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) condition FROM clinicaltrial_2021
WHERE Conditions IS NOT NULL)
GROUP BY condition)

SELECT m.code, SUM(y.frequency) frequency
FROM y
LEFT JOIN mesh m
ON y.condition = m.term
GROUP BY m.code
ORDER BY frequency DESC 
limit 5;

-- COMMAND ----------

WITH y AS (
SELECT condition, COUNT(*) frequency
FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) condition FROM clinicaltrial_2021
WHERE Conditions IS NOT NULL)
GROUP BY condition)

SELECT m.code, SUM(y.frequency) frequency
FROM y
LEFT JOIN mesh m
ON y.condition = m.term
GROUP BY m.code
ORDER BY frequency DESC 
limit 10;

-- COMMAND ----------

--#Q5 = "Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies."

SELECT Sponsor, COUNT(*) frequency
FROM clinicaltrial_2021
WHERE Sponsor NOT IN (SELECT DISTINCT(Parent_Company) FROM pharma)
GROUP BY Sponsor
ORDER BY frequency DESC 
limit 10;

-- COMMAND ----------

--#Q6 = "Plot number of completed studies each month in a given year – for the submission dataset, the year is 2021. You need to include your visualization as well as a table of all the values you have plotted for each month" 

SELECT Completion, COUNT(*) frequency
FROM clinicaltrial_2021
WHERE Status == 'Completed' AND Completion LIKE '%2021'
GROUP BY Completion
ORDER BY frequency DESC;

-- COMMAND ----------

--#To visualize table using in-built Pie Chart in databricks

SELECT Completion, COUNT(*) frequency
FROM clinicaltrial_2021
WHERE Status == 'Completed' AND Completion LIKE '%2021'
GROUP BY Completion
ORDER BY frequency DESC;

-- COMMAND ----------


