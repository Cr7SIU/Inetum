from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, TimestampType
from pyspark.sql import functions as F

def to_timestamp_space_flight(dataFrame, columnName):
    time_regex = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
    timeStamp_format = 'yyyy-MM-dd HH:mm:ss'
    
    return dataFrame.withColumn(
                        columnName, 
                        F.to_timestamp(F.regexp_replace(F.regexp_extract(columnName, time_regex, 0), 'T', ' '), timeStamp_format)
                        )
    
def create_data_frame(data, sparkSesionName: str, schema: StructType):
    spark = (
        SparkSession
        .builder
        .appName(f'SpaceFlight{sparkSesionName}')
        .getOrCreate()
    )
    
    rdd = spark.sparkContext.parallelize(data)
    df = spark.createDataFrame(rdd, schema)
    
    return df

def general_schema(response_schema: list):
    general_schema_values = {
        "id" : StructField("id", StringType(), True),
        "title" : StructField("title", StringType(), True),
        "authors" : StructField("authors", ArrayType(StringType()), True),
        "url" : StructField("url", StringType(), True),
        "image_url" : StructField("image_url", StringType(), True),
        "news_site" : StructField("news_site", StringType(), True),
        "summary" : StructField("summary", StringType(), True),
        "published_at" : StructField("published_at", StringType(), True),
        "updated_at" : StructField("updated_at", StringType(), True),
        "featured" : StructField("featured", StringType(), True),
        "launches" : StructField("launches", ArrayType(StringType()), True),
        "events" : StructField("events", ArrayType(StringType()), True)
    }
    
    spark_schema = []
    
    for column in response_schema:
        spark_schema.append(general_schema_values[column])
        
    return spark_schema


