import logging
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq

from dotenv import load_dotenv
from quixstreams import Application, State, message_key

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.environ("aws_access_key_id"),
    aws_secret_access_key=os.environ("aws_access_key")
)

def write_pandas_parquet_to_s3(df, bucketName, keyName, fileName):
    # dummy dataframe
    table = pa.Table.from_pandas(df)
    pq.write_table(table, fileName)

    # upload to s3
    s3 = boto3.client("s3")
    with open(fileName) as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=bucketName, Key=keyName)

def upload_row(row: dict):
    df = pd.DataFrame.from_dict(row, orient='index')
    write_pandas_parquet_to_s3(
        df, os.environ("s3_bucket"), "test/file.parquet", ".tmp/file.parquet")
    
    return row

app = Application.Quix(
    "s3_sink",
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with Quix deserializer
input_topic = app.topic(os.environ("input"), value_deserializer="json")

# Create a StreamingDataFrame and start building your processing pipeline
sdf = app.dataframe(input_topic)

# Print the transformed message to the console
sdf.apply(upload_row)

if __name__ == "__main__":
    # Start message processing
    app.run(sdf)