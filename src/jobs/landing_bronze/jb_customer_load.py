from src.utils.job_utils import Job

from pyspark.sql.functions import col,lit,current_timestamp


root_path = '/Volumes/gizmobox/landing/operational_data/customers_2024_*.json'

def customer_direct_load():
    customer_lnd_df = spark.read.format('json').load(root_path)
    customer_lnd_df.select(
        "*",
        col("_metadata.file_path").alias("source_file_path"),
        col("_metadata.file_size").alias("file_size_bytes"),
        current_timestamp().alias("load_timestamp"),
        lit("customer").alias("load_source"),
        lit("landing").alias("schema")
    )
    display(customer_lnd_df)

customer_direct_load()




    
