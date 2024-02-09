import os
import pandas as pd
import boto3
import awswrangler as wr

from quixstreams import Application, State
from quixstreams.models.serializers.quix import JSONDeserializer, JSONSerializer


my_session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="eu-west-2"
)

# Create an Application.
app = Application.Quix(
        consumer_group="s3_sink",
        auto_offset_reset="earliest",
        auto_create_topics=True,  # Quix app has an option to auto create topics
    )

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())

sdf = app.dataframe(input_topic)

def upload_to_s3(row: dict):

    # df = pd.DataFrame.from_dict(row, orient='index')

    # wr.s3.to_parquet(df, "s3://hackathon-quix-tun/data/df.parquet")
    df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=df,
        path="s3://hackathon-quix-tun/dataset/",
        dataset=True,
        database="my_db",
        table="my_table",
        boto3_session=my_session
    )

    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(upload_to_s3)

if __name__ == "__main__":
    app.run(sdf)
