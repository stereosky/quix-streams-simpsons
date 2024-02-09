import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from quixstreams import Application, State
from quixstreams.models.serializers.quix import JSONDeserializer, JSONSerializer

s3 = boto3.client(
    "s3",
    aws_access_key_id = os.environ["aws_access_key_id"],
    aws_secret_access_key = os.environ["aws_access_key"]
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

    df = pd.DataFrame.from_dict(data, orient='index')

    table = pa.Table.from_pandas(df)
    pq.write_table(table, ".tmp/file.parquet")

    with open(fileName) as f:
       object_data = f.read()
       s3.put_object(Body=object_data, Bucket=os.environ["s3_bucket"], Key="test/file.parquet")

    
    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(upload_to_s3)

if __name__ == "__main__":
    app.run(sdf)
