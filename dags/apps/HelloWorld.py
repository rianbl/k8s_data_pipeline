from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    hello_world_df = spark.createDataFrame([("Hello", "World")], ["Greeting", "Target"])
    hello_world_df.show()
    spark.stop()
