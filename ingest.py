from pyspark.sql.types import StructType, StructField, IntegerType, FloatType,StringType

schema_train = StructType([
    StructField("id", IntegerType(), True),
    StructField("week", IntegerType(), True),
    StructField("center_id", IntegerType(), True),
    StructField("meal_id", IntegerType(), True),
    StructField("checkout_price", FloatType(), True),
    StructField("base_price", FloatType(), True),
    StructField("emailer_for_promotion", IntegerType(), True),
    StructField("homepage_featured", IntegerType(), True),
    StructField("num_orders", IntegerType(), True)
])

schema_meal = StructType([
    StructField("meal_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("cuisine", StringType(), True)
])

schema_center = StructType([
    StructField("center_id", IntegerType(), True),
    StructField("city_code", IntegerType(), True),
    StructField("region_code", IntegerType(), True),
    StructField("center_type", StringType(), True),
    StructField("op_area", FloatType(), True)
])


class Ingest:

    def __init__(self, spark):
        # A class level variable
        self.spark = spark
        # access_key = ''
        # secret_key = ''
        # sc = self.spark.sparkContext
        # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

        # If you are using Auto Loader file notification mode to load files, provide the AWS Region ID.
        # aws_region = ""
        # sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

    def ingest_data_train(self):
        print("Ingesting from csv")
        customer_df = self.spark.read.format("csv").option("header", "true").schema(schema_train).load("s3://mybucketetlpiyush/train_new.csv") 
        return customer_df
    
    def ingest_data_meal(self):
        print("Ingesting from csv")
        customer_df = self.spark.read.format("csv").option("header", "true").schema(schema_meal).load("s3://mybucketetlpiyush/meal_info.csv")
        return customer_df
    
    def ingest_data_center(self):
        print("Ingesting from csv")
        customer_df = self.spark.read.format("csv").option("header", "true").schema(schema_center).load("s3://mybucketetlpiyush/fulfilment_center_info.csv")
        return customer_df
