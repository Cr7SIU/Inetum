import sys
sys.path('Intenium')
from pipeline_processing.fetch_data import *
from pipeline_processing.dataframe_processing import *
from pipeline_processing.aws import *
from datetime import datetime

def main(ENDPOINT):
    data = fetch_data(endpoint = ENDPOINT)
    
    # Extract the schema from the fetched data
    response_schema = list(data[0].keys())
    
    schema_structured = general_schema(response_schema)
    
    df = create_data_frame(data, sparkSesionName= ENDPOINT , schema_structured = schema_structured)

    df = to_timestamp_space_flight(df, 'published_at')
    df = to_timestamp_space_flight(df, 'updated_at')

    df = df.dropDuplicates(["id"])
    
    # Define the local path to store the parquet file
    local_path = f"/{ENDPOINT}_tmp/{ENDPOINT}_table.parquet"

    df.write.parquet(local_path)
    
    save_dataframe_to_s3(local_path)