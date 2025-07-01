import praw, json, time
reddit = praw.Reddit(client_id='24eDEbh2CZXjgBhuCJTg8Q', client_secret='LFaqdDfr-djUtlXckTQq2o-bSI3znA', user_agent='topic-modeler')
from kafka import KafkaProducer
from json import dumps

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC             = 'reddit_posts'
MAX_RESULTS       = 100

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    acks='all',
    retries=5,
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

for submission in reddit.subreddit('all').stream.submissions(skip_existing=True):
    data = {
        'id': submission.id,
        'title': submission.title,
        'selftext': submission.selftext,
        'created_utc': submission.created_utc
    }
    
    # gửi dict, serializer sẽ tự JSON + encode
    future = producer.send('reddit_posts', data)
    producer.flush()  # đảm bảo đã gửi xong

    # --- Dòng in ra kết quả ---
    # In ra dictionary data
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Sent data:", data)

    # (Nếu cần in metadata của bản tin vừa gửi)
    try:
        meta = future.get(timeout=10)
        print(f" → topic={meta.topic}, partition={meta.partition}, offset={meta.offset}")
    except Exception as e:
        print(" → Failed to get metadata:", e)

    time.sleep(5)