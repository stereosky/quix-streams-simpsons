import logging
import boto3
from botocore.exceptions import ClientError
import os

from dotenv import load_dotenv
from quixstreams import Application, State, message_key

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.environ("aws_access_key_id"),
    aws_secret_access_key=os.environ("aws_access_key")
)

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

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
sdf = sdf.update(lambda val: print(f"Sending update: {val}"))

if __name__ == "__main__":
    # Start message processing
    app.run(sdf)