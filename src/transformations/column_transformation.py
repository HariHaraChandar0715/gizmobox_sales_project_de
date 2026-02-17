def add_columns(df):
    column_names = {
        '_c0':'payment_id','_c1':'order_id',
        '_c2':'payment_date','_c3':'payment_status',
        '_c4':'payment_method'
    }
    for old_name, new_name in column_names.items():
        df = df.withColumnRenamed(old_name,new_name)

    return df

