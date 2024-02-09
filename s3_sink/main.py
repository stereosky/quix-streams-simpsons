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
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())

sdf = app.dataframe(input_topic)

# Add custom badwords here and 
custom_badwords = ['shorts']
profanity.add_censor_words(custom_badwords)

def count_and_replace_profanity(row: dict, state: State):

    dialogue = row["spoken_words"]

    if dialogue != None and profanity.contains_profanity(dialogue):

        row["censored_words"] = profanity.censor(dialogue)
    
    # return the updated row so more processing can be done on it
    return row

# apply the result of the count_names function to the row
sdf = sdf.apply(count_and_replace_profanity, stateful=True)

# Filter schema
sdf = sdf[sdf.contains("censored_words")]

# print the row with this inline function
sdf = sdf.update(lambda row: print(row))

# publish the updated row to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)
