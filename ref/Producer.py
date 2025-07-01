
from googleapiclient.discovery import build
from kafka import KafkaProducer
from json import dumps
from time import sleep
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yt-producer")


API_KEY   = 'AIzaSyDOpym1qAAyNK9WHg2X3RJKa54fDhgb-r8'
VIDEO_ID  = '_P4z6H_UV9E'

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC             = 'youtube-comments'
MAX_RESULTS       = 100


youtube = build('youtube', 'v3', developerKey=API_KEY)

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    acks='all',
    retries=5,
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

def fetch_comments(video_id):
    next_page_token = None
    fetched = 0
    while True:
        resp = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText',
            maxResults=min(100, MAX_RESULTS - fetched),
            pageToken=next_page_token
        ).execute()

        for item in resp.get('items', []):
            snip = item['snippet']['topLevelComment']['snippet']
            yield {
                "comment_id"   : snip['authorChannelId'].get('value','') + "_" + snip['authorDisplayName'],
                "author"       : snip['authorDisplayName'],
                "text"         : snip['textDisplay'],
                "published_at" : snip['publishedAt'],
                "like_count"   : snip['likeCount']
            }
            fetched += 1
            if fetched >= MAX_RESULTS:
                return

        next_page_token = resp.get('nextPageToken')
        if not next_page_token or fetched >= MAX_RESULTS:
            break

if __name__ == "__main__":
    logger.info(f"Starting continuous fetching comments for video {VIDEO_ID}")
    while True:
        try:
            for comment in fetch_comments(VIDEO_ID):
                producer.send(TOPIC, comment)
                logger.info(f"Sent {comment['comment_id']}")
                time.sleep(1)  # nghỉ 1 giây giữa mỗi comment để tránh spam

            logger.info("Sleeping 60s before next fetch...")
            time.sleep(60)  # Chờ 1 phút rồi fetch tiếp comment mới
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            time.sleep(30)  # Nếu lỗi, chờ 30s rồi thử lại

    producer.close()