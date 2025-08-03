

# not working 


import random
import os
import re
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf, current_timestamp
from pyspark.sql.types import StringType

# === Constants ===
LOG_FILE = "./ml-100k/bluesky.jsonl"

# === Initialize Spark Session ===
spark = SparkSession.builder \
    .appName("BlueskyHashtags") \
    .getOrCreate()

# === Load log lines once on driver and broadcast ===
if not os.path.exists(LOG_FILE):
    raise FileNotFoundError(f"{LOG_FILE} not found")

with open(LOG_FILE, "r", encoding="utf-8") as f:
    lines = [line.strip() for line in f if line.strip()]

if not lines:
    raise ValueError("The log file is empty.")

broadcast_lines = spark.sparkContext.broadcast(lines)

# === UDF: Return random log line from broadcasted data ===
@udf(StringType())
def get_random_log_line():
    data = broadcast_lines.value
    return random.choice(data) if data else None

# === UDTF: Extract hashtags from log line ===
@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, json_line: str):
        try:
            data = json.loads(json_line)
            text = data.get("text", "")
            hashtags = re.findall(r"#\w+", text)
            for tag in hashtags:
                yield (tag.lower(),)
        except:
            pass

# Register the UDTF
spark.udtf.register("extract_hashtags", HashtagExtractor)

# === Create rate stream source ===
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load()

# === Enrich with JSON and timestamp ===
posts_df = rate_df.withColumn("json", get_random_log_line()) \
                  .withColumn("eventtime", current_timestamp())

# Register temp view
posts_df.createOrReplaceTempView("raw_posts")

# === SQL to extract hashtags ===
hashtags_query = """
    SELECT
        hashtag,
        eventtime
    FROM raw_posts,
    LATERAL extract_hashtags(json)
"""

hashtags_df = spark.sql(hashtags_query)
hashtags_df.createOrReplaceTempView("hashtags")

# === SQL to compute top hashtags ===
top_hashtags_df = spark.sql("""
    SELECT hashtag, COUNT(*) AS count
    FROM hashtags
    WHERE hashtag IS NOT NULL
    GROUP BY hashtag
    ORDER BY count DESC
    LIMIT 10
""")

# === Output to console ===
query = top_hashtags_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("top_hashtags") \
    .start()

query.awaitTermination()
