from collections import defaultdict
from pyspark.sql.functions import input_file_name,col

catalog_root_path = '/Volumes/gizmobox/'
landing_operational_root_path = '/Volumes/gizmobox/landing/operational_data/'

landing_customer_root_path = '/Volumes/gizmobox/landing/operational_data/customers'
bronze_operational_root_path = '/Volumes/gizmobox/bronze/operational_data/'




def get_consolidated_customer_file_paths(spark,landing_customer_root_path):

    consolidated_file_paths = defaultdict(list)

    try:
        customer_df = spark.read.format('json').load(landing_customer_root_path)
        customers_file_paths = customer_df.select(col("_metadata.file_path").alias("path")).distinct()

        customer_file_paths = customers_file_paths.collect()
        customer_file_counts = len(customer_file_paths)

        if customer_file_counts == 0:
            print('No new files in landing layer')
        else:
            for file_info in customer_file_paths:
                path = file_info.path
                year = path.split('/')[-1].split('_')[1]
                consolidated_file_paths[year].append(path)

    except Exception as error:
        
        print(f'Error Occured : {error}')
        consolidated_file_paths = 0

    return consolidated_file_paths