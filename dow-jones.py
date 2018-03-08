# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

dow_jones_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tmp/climate/dow-jones.csv")

median_household_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tmp/climate/median-household.csv")

pincode_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tmp/climate/dataToVisualise.csv")

