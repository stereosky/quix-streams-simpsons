import os
from quixstreams import Application, State
from quixstreams.models.serializers.quix import JSONDeserializer, JSONSerializer

from better_profanity import profanity


# import our get_app function to help with building the app for local/Quix deployed code
from app_factory import get_app

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv(override=False)

# Create an Application.
app = Application.Quix(
            consumer_group="s3_sink",
            auto_offset_reset="earliest",
            auto_create_topics=True,  # Quix app has an option to auto create topics
        )

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())

sdf = app.dataframe(input_topic)

def upload_to_s3(row: dict):

    print(f"********** row: {row}")
    
    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(upload_to_s3)

if __name__ == "__main__":
    app.run(sdf)
