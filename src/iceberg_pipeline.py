from pyspark.sql import SparkSession
import os

def create_spark_session():
    print("Initializing Spark Session with Apache Iceberg extensions...")
    spark = SparkSession.builder \
        .appName("Iceberg-Zero-ETL-Pipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", os.path.join(os.getcwd(), "warehouse")) \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Checkpoint 1: Create an Iceberg table
    spark.sql("CREATE TABLE IF NOT EXISTS local.db.events (id BIGINT, event_name STRING, data STRING) USING iceberg")
    print("Iceberg table 'local.db.events' verified/created.")
    
    # Write dummy data
    spark.sql("INSERT INTO local.db.events VALUES (1, 'user_signup', 'raw_log_data_here')")
    
    # Read back to verify
    df = spark.sql("SELECT * FROM local.db.events")
    df.show()
    
    print("Phase 1 Step 2 Validation: Iceberg table written and read successfully.")
