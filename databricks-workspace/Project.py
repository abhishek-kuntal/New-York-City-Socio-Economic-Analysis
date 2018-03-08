# Databricks notebook source

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

crime_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/tables/crime.csv")
climate_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/tables/dataset/1144508.csv")

# COMMAND ----------

len(climate_df.columns)

# COMMAND ----------

  crime_df.select("LAW_CAT_CD").distinct().show()

# COMMAND ----------

crime_df.select("OFNS_DESC").distinct().count()

# COMMAND ----------

crime_df.sort("CMPLNT_FR_DT").na.drop().show()

# COMMAND ----------

# Date conversion

from pyspark.sql.functions import udf


crime_df_date = crime_df.select("CMPLNT_FR_DT", "CMPLNT_FR_TM", "OFNS_DESC", "LAW_CAT_CD", "Latitude", "Longitude", "PD_DESC")

def crime_date_convert(date, time):
  try:
  	k = date.split("/")
	full_date = "-".join(k[2:] + k[1:2] + k[0:1])
	full_date = full_date + " " + time.split(":")[0]
	return full_date
  except:
    return "2021-01-01 00"
  


udf_crime_date_convert = udf(crime_date_convert, StringType())

with_crime_date_conversion = crime_df_date.withColumn("normalized_date", udf_crime_date_convert("CMPLNT_FR_DT", "CMPLNT_FR_TM")).select("normalized_date", "OFNS_DESC", "LAW_CAT_CD", "Latitude", "Longitude", "PD_DESC")



climate_df_date = climate_df.select("DATE", "HOURLYDRYBULBTEMPC")



# COMMAND ----------

def climate_date_convert(date):
	return date.split(":")[0]

udf_climate_date_convert = udf(climate_date_convert, StringType())

climate_with_date_conversion = climate_df_date.withColumn("normalized_date", udf_climate_date_convert("DATE")).select("normalized_date", "HOURLYDRYBULBTEMPC")

join_climate_crime = climate_with_date_conversion.join(with_crime_date_conversion, "normalized_date")

# COMMAND ----------

cleaned_join_climate_crime = join_climate_crime.na.drop()

def get_hour(date):
	return date.split(" ")[1]

def get_date(date):
	return date.split(" ")[0]


udf_get_hour = udf(get_hour, StringType())

udf_get_date = udf(get_date, StringType())


with_refined_date_columns = cleaned_join_climate_crime.withColumn("date", udf_get_date("normalized_date")).withColumn("hour", udf_get_hour("normalized_date"))

# COMMAND ----------

import requests
from pyspark.sql.types import StringType

def get_pincode(lat, lng):
  try:
	sensor = 'true'
	base = "https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyD0Hojtoe0t06aY39dcD1vV77Lsxa3PW6c&"
	params = "latlng={lat},{lon}&sensor={sen}".format(lat=lat,lon=lng,sen=sensor)
	url = "{base}{params}".format(base=base, params=params)
	response = requests.get(url).json()
	results = response["results"]
	address_components = list(map(lambda x: x["address_components"], results))
	flatten = [item for sublist in address_components for item in sublist]
	postal_objects = list(filter(lambda x: x["types"] == ["postal_code"], flatten))
	postal_codes = list(map(lambda x: x["long_name"], postal_objects))
	return str(postal_codes[0]);
  except Exception as e:
    return None


udf_getZip = udf(get_pincode, StringType())

# COMMAND ----------

with_zipCode=with_refined_date_columns.groupBy("Latitude","Longitude").count().withColumn("zipcode", udf_getZip("Latitude", "Longitude"))

# COMMAND ----------

zipcodeData= sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/tables/withZipcode.csv")

# COMMAND ----------

dataToVisualize=zipcodeData.join(with_refined_date_columns,[with_refined_date_columns.Latitude==zipcodeData.Latitude,with_refined_date_columns.Longitude==zipcodeData.Longitude]).select(
   with_refined_date_columns.Latitude,with_refined_date_columns.Longitude,"normalized_date", "OFNS_DESC", "LAW_CAT_CD", "PD_DESC",zipcodeData.zipcode,"HOURLYDRYBULBTEMPC","date","hour")

# COMMAND ----------

  df= sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/dataToVis.csv")

# COMMAND ----------

display(crimeWithWeather)


# COMMAND ----------

crimeWithWeather=df.sort("HOURLYDRYBULBTEMPC")


# COMMAND ----------

hotel=sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/tables/HotelData.csv")

# COMMAND ----------

hotelWithCrime=hotel.join(crimeWithWeather,hotel.zipCode==crimeWithWeather.zipcode).select(hotel.zipCode,"Name","LAW_CAT_CD","OFNS_DESC","normalized_date").na.drop()

# COMMAND ----------

hotel.columns

# COMMAND ----------

display(hotelWithCrime)

# COMMAND ----------

bars=sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/FileStore/tables/Liquor_Authority_Quarterly_List_of_Active_Licenses.csv")


# COMMAND ----------

bars.columns

# COMMAND ----------

barsWithCrime=bars.join(crimeWithWeather,bars.Zip==crimeWithWeather.zipcode).select(bars.Zip,"Premises Name",bars.Latitude,bars.Longitude,"LAW_CAT_CD","OFNS_DESC","normalized_date").na.drop()

# COMMAND ----------

display(barsWithCrime)

# COMMAND ----------

bars=sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tables/FileStore/abc.csv")


# COMMAND ----------

display(crime_df)

# COMMAND ----------

display(climate_df)

# COMMAND ----------


