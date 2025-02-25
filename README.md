# dual-write-iceberg-s3
dual-write-iceberg-s3

![image](https://github.com/user-attachments/assets/a94eea0b-0a95-4f02-a27e-fc3e3e936c5d)

#### Spark Job
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def create_spark_session():
    return SparkSession.builder.getOrCreate()

def setup_iceberg_table(spark, catalog, namespace, table, table_type):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")
    spark.sql(f'''
    CREATE TABLE IF NOT EXISTS `{catalog}`.`{namespace}`.`{table}` (
        id INT,
        name STRING,
        age INT,
        created_at TIMESTAMP
    ) USING iceberg
    ''')

def insert_mock_data(spark, catalog, namespace, table):
    mock_data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    df = spark.createDataFrame(mock_data, ["id", "name", "age"]).withColumn("created_at", current_timestamp())
    df.write.format("iceberg").mode("append").saveAsTable(f"{catalog}.{namespace}.{table}")

def main():
    settings = [
        {"catalog_name": "ManagedIcebergCatalog", "namespace": "test_ns", "table": "sample_table", "type": "ManagedIceberg"},
        {"catalog_name": "UnManagedIcebergCatalog", "namespace": "test_ns", "table": "sample_table", "type": "UnManagedIceberg"}
    ]
    spark = create_spark_session()
    for setting in settings:
        setup_iceberg_table(spark, setting["catalog_name"], setting["namespace"], setting["table"], setting["type"])
        insert_mock_data(spark, setting["catalog_name"], setting["namespace"], setting["table"])
        print(f"Table {setting['catalog_name']}.{setting['namespace']}.{setting['table']} written successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
```

## Spark Submit 
```
park-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1, \
              com.amazonaws:aws-java-sdk-bundle:1.12.661, \
              org.apache.hadoop:hadoop-aws:3.3.4, \
              software.amazon.awssdk:bundle:2.29.38, \
              software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.UnManagedIcebergCatalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.UnManagedIcebergCatalog.warehouse=s3://your-bucket/warehouse/ \
    --conf spark.sql.catalog.UnManagedIcebergCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.UnManagedIcebergCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.ManagedIcebergCatalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ManagedIcebergCatalog.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
    --conf spark.sql.catalog.ManagedIcebergCatalog.warehouse=arn:aws:s3tables:us-east-1:YOUR_AWS_ACCOUNT:bucket/iceberg-managed-tables \
    --conf spark.sql.catalog.ManagedIcebergCatalog.client.region=us-east-1 \
    s3://your-bucket/scripts/dual_write_iceberg_table_buckets.py

```
