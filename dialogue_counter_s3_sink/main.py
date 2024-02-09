import os
import pandas as pd
import boto3
import awswrangler as wr

from quixstreams import Application
from quixstreams.models.serializers.quix import JSONDeserializer


my_session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ["AWS_REGION"]
)

s3_bucket = os.environ["s3_bucket"]

# Create an Application.
app = Application.Quix(
        consumer_group="s3_sink",
        auto_offset_reset="earliest",
        auto_create_topics=True,  # Quix app has an option to auto create topics
    )

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())

sdf = app.dataframe(input_topic)

def upload_to_s3(row: dict):

    character_name = row['raw_character_text']
    path = f"s3://{s3_bucket}/simpsons/counts/{character_name}.parquet"
    
    print(f"Writing to {path}")
    # Storing data in data lake
    wr.s3.to_parquet(
        df=pd.DataFrame(row['count'], index=[0]),
        path=path,
        boto3_session=my_session
    )

    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(upload_to_s3)

if __name__ == "__main__":
    app.run(sdf)
