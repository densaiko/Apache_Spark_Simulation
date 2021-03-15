from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud import bigquery
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkFiles
import timeit
import os
import warnings
warnings.filterwarnings('ignore')

if __name__ == "__main__":
    start_all = timeit.default_timer()
    start = timeit.default_timer()
    
    #Load the data using spark
    spark = SparkSession.builder.appName("Testing").getOrCreate()
    spark.sparkContext.addFile("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv")

    df_big = (spark.read.format('csv')
     .option('header','true')
     .option('inferSchema','true')
     .load(SparkFiles.get('yellow_tripdata_2020-01.csv')))
    
    stop = timeit.default_timer()
    print('Time for reading data: ', stop - start)
    
    #data cleansing
    start = timeit.default_timer()
    df_big.createOrReplaceTempView('yellowtaxi')

    query = '''
    SELECT *
    FROM yellowtaxi
    WHERE `VendorID` = 2 and `passenger_count` >= 2
    '''

    df_big_clean = spark.sql(query)
    stop = timeit.default_timer()
    print('Time for querying data: ', stop - start)

    #transform pyspark dataframe to pandas dataframe
    start = timeit.default_timer()
    df_big_pandas = df_big_clean.toPandas()
    stop = timeit.default_timer()
    print('Time for transforming data to pandas dataframe: ', stop - start)
    
    #upload to cloud storage
    start = timeit.default_timer()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="datafellowship-307406-256c5427d8de.json"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('practice_densaiko_1')
    data_terbaru=bucket.blob("data_spark.csv")

    data_terbaru.upload_from_string(df_big_pandas.to_csv(), 'text/csv') ##upload pandas dataframe to storage

    stop = timeit.default_timer()
    print('Time to upload data: ', stop - start)
    stop_all = timeit.default_timer()
    print('Overall Time: ', stop_all - start_all)