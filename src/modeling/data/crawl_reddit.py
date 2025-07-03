import praw, time, os
import csv
from langdetect import detect, DetectorFactory

# Khởi tạo Reddit client
reddit = praw.Reddit(
    client_id='24eDEbh2CZXjgBhuCJTg8Q',
    client_secret='LFaqdDfr-djUtlXckTQq2o-bSI3znA',
    user_agent='topic-modeler'
)

CSV_FILE = "reddit_posts.csv"
# Kiểm tra xem đã có file chưa để chỉ ghi header 1 lần
file_exists = os.path.exists(CSV_FILE)
subreddits = ["anime", "gaming", "AskReddit", "teenagers", "sports", "worldnews", "Music"]
subreddit_str = "+".join(subreddits)

# Mở file ở chế độ append
with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["id", "text"])
    # Nếu file chưa có, ghi header trước
    if not file_exists:
        writer.writeheader()
        csvfile.flush()

    for subreddit_name in subreddits:
        print(f"=== Fetching up to 10k hot posts from r/{subreddit_name} ===")
        count = 0
        
        # PRAW chỉ trả tối đa ~1000 hot posts, nhưng ta vẫn đặt limit=10_000
        for sub in reddit.subreddit(subreddit_name).hot(limit=10_000):
            text = f"{sub.title} {sub.selftext or ''}".strip()
            
            try:
                if detect(text) != 'en':
                    continue
            except Exception:
                continue
            
            row = {
                "id": sub.id,
                "text": sub.title + " " + sub.selftext
            }
            writer.writerow(row)
            # flush để ghi ngay ra đĩa
            csvfile.flush()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Appended post {sub.id}")
        
        
        