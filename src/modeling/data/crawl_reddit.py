import praw, time, os
import csv

# Khởi tạo Reddit client
reddit = praw.Reddit(
    client_id='24eDEbh2CZXjgBhuCJTg8Q',
    client_secret='LFaqdDfr-djUtlXckTQq2o-bSI3znA',
    user_agent='topic-modeler'
)

CSV_FILE = "reddit_posts.csv"
# Kiểm tra xem đã có file chưa để chỉ ghi header 1 lần
file_exists = os.path.exists(CSV_FILE)

# Mở file ở chế độ append
with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["id", "text"])
    # Nếu file chưa có, ghi header trước
    if not file_exists:
        writer.writeheader()
        csvfile.flush()

    # Stream từng submission và append vào CSV
    for sub in reddit.subreddit("books").hot(limit=1000):
        row = {
            "id": sub.id,
            "text": sub.title + " " + sub.selftext
        }
        writer.writerow(row)
        # flush để ghi ngay ra đĩa
        csvfile.flush()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Appended post {sub.id}")