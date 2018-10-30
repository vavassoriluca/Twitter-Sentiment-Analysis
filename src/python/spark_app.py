from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'


if __name__ == "__main__":
    
    zkQuorum = "localhost:2181"
    topic = "twitter"

    # User-supplied command arguments
    if len(sys.argv) != 3:
        print("Usage: spark-stream-tweets.py <min_hashtag_counts> <seconds_to_run>")
        exit(-1)
    min_hashtag_counts = int(sys.argv[1])
    seconds_to_run = int(sys.argv[2])

    sc = SparkContext("local[2]", appName="Twitter")
    ssc = StreamingContext(sc, seconds_to_run)

    tweets = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    # Tweet processing. 
    # Kafka passes a tuple of message ID and message text. Message text is the tweet text.
    # All tweets are turned into ([people],[hashtags]) and tweets without hashtags are filtered
    # out.

    # Returns ([people], [hashtags])
    lines = tweets.map(lambda x: get_people_with_hashtags(x[1])).filter(lambda x: len(x)>0)

    lines.pprint(10)

    ssc.start()
    ssc.awaitTermination()