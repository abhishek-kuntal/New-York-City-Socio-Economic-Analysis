from math import *
from pyspark.sql.functions import explode, avg, udf
from pyspark.sql.types import StringType

business_df = sqlContext.read.json("../../business.json")

alcohol_bars_df = business_df.filter(business_df["attributes.Alcohol"] != "none")

NY_CENTER_LAT = 40.7128
NY_CENTER_LONG = -74.0060

def from_center(lat, lng):
    try:
        center_lat = radians(NY_CENTER_LAT)
        center_lng = radians(NY_CENTER_LONG)
        lat_rad = radians(float(lat))
        lng_rad = radians(float(lng))
        dist = acos((sin(lat_rad) * sin(center_lat)) + (cos(lat_rad) * cos(center_lat) * cos(lng_rad - center_lng))) * 6371
        return dist < 1000
    except:
        return False

udf_from_center = udf(from_center, StringType())

with_from_center = alcohol_bars_df.withColumn("within_radius", udf_from_center("latitude", "longitude"))

within_new_york = with_from_center.filter(with_from_center.within_radius == "true")