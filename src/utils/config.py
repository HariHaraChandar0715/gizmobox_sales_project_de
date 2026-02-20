from collections import defaultdict
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import input_file_name,col

catalog_root_path = '/Volumes/gizmobox/'
landing_operational_root_path = '/Volumes/gizmobox/landing/operational_data/'

landing_customer_root_path = '/Volumes/gizmobox/landing/operational_data/customers'
bronze_operational_root_path = '/Volumes/gizmobox/bronze/operational_data/'




def get_consolidated_customer_file_paths(
    spark : SparkSession,
    landing_customer_root_path : str
) -> DataFrame:

    landing_customer_file_paths = defaultdict(list)
    consolidated_file_paths = list()

    try:
        customer_df = spark.read.format('json').load(landing_customer_root_path)
        customers_file_paths = customer_df.select(col("_metadata.file_path").alias("path")).distinct()

        customer_file_paths = customers_file_paths.collect()
        customer_file_counts = len(customer_file_paths)

        if customer_file_counts == 0:

            print('No new files in landing layer')
            consolidated_file_paths = None
            return consolidated_file_paths
        
        else:

            for file_info in customer_file_paths:
                path = file_info.path
                year = int(path.split('/')[-1].split('_')[1])
                landing_customer_file_paths[year].append(path)


        landing_customer_file_paths = dict(landing_customer_file_paths)
        year_keys = list(landing_customer_file_paths.keys())
        #print(landing_customer_file_paths)
        print('loop execution starts')
        for year_key in year_keys:
            print('outer loop starts')

            for file_path in landing_customer_file_paths[year_key]:
                month = 0
                path_buff = list()
                path_buff.append(year_key)
                temp_file_path = file_path
                month = int(temp_file_path.split('/')[-1].split('.')[0].split('_')[2])
                path_buff.append(month)
                path_buff.append(file_path)
                consolidated_file_paths.append(tuple(path_buff))

        consolidated_file_paths = spark.createDataFrame(
            consolidated_file_paths,
            ['year','month','lnd_opt_cst_file_paths']
        )


    except Exception as error:
        
        print(f'Error Occured : {error}')
        consolidated_file_paths = None

    

    return consolidated_file_paths