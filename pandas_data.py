from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud import bigquery
import pandas as pd
import timeit
import os
import warnings
warnings.filterwarnings('ignore')

if __name__ == "__main__":
    start_all = timeit.default_timer()
    start = timeit.default_timer()
    
    #read the file
    data_pandas = pd.read_csv("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv")
    stop = timeit.default_timer()
    print('Time for reading data: ', stop - start)
    
    #data cleansing
    start = timeit.default_timer()
    data_pandas_clean = data_pandas[(data_pandas['VendorID'] == 2) & (data_pandas['passenger_count'] >= 2)]
    stop = timeit.default_timer()
    print('Time for querying data: ', stop - start)
    
    
    #upload to cloud storage
    start = timeit.default_timer()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="datafellowship-307406-256c5427d8de.json"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('practice_densaiko_1')
    data_terbaru=bucket.blob("data_pandas.csv")

    data_terbaru.upload_from_string(data_pandas_clean.to_csv(), 'text/csv')
    
    stop = timeit.default_timer()
    print('Time to upload data: ', stop - start)
    stop_all = timeit.default_timer()
    print('Overall Time: ', stop_all - start_all)