import os
import logging
from kafka import KafkaConsumer
from json import loads

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yt-consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC             = os.getenv("KAFKA_TOPIC", "youtube-comments")
GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "yt-comment-group")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    logger.info(f"Subscribed to {TOPIC}, waiting for messages…")
    try:
        for msg in consumer:
            comment = msg.value
            logger.info(f"[{msg.partition}@{msg.offset}] {comment['comment_id']}: {comment['text'][:50]}…")
    except KeyboardInterrupt:
        logger.info("Stopping consumer.")
    finally:
        consumer.close()
