import pandas as pd
import re
from langdetect import detect, DetectorFactory

# Ensure langdetect produces consistent results
DetectorFactory.seed = 0

# 1. Đọc dữ liệu
df = pd.read_csv("reddit_posts.csv", encoding="utf-8")

# 2. Loại bỏ duplicate theo id
df = df.drop_duplicates(subset="id")

# 3. Định nghĩa hàm clean
def clean_text(text: str) -> str:
    # 3.1 chuyển về string (phòng None)
    text = str(text)
    # 3.1 remove URLs (http://, https://, www.)
    text = re.sub(r'http[s]?://\S+|www\.\S+', ' ', text)
    # 3.2 xóa ký tự đặc biệt, chỉ giữ chữ, số và khoảng trắng
    text = re.sub(r'[^A-Za-z0-9\s]', ' ', text)
    # 3.3 collapse nhiều whitespace/newline thành 1 space
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# 4. Áp dụng clean và tạo cột mới
df['clean_text'] = df['text'].apply(clean_text)

# 5. Lọc bỏ posts dưới 10 từ
df = df[df['clean_text'].str.split().str.len() >= 10]

# 6. Ghi ra file mới
df.to_csv("reddit_posts_clean.csv", index=False, encoding="utf-8")

print(f"Đã làm sạch và lưu {len(df)} bản ghi vào reddit_posts_clean.csv")