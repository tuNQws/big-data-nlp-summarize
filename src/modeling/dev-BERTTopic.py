import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from sentence_transformers import SentenceTransformer
from umap import UMAP
from hdbscan import HDBSCAN
# topics là mảng int, trong đó -1 = outlier
import numpy as np

# 1.1 Đọc data đã clean
df = pd.read_csv("data/reddit_posts_clean.csv", encoding="utf-8")
df = df.dropna(subset=["clean_text"])
df["clean_text"] = df["clean_text"].astype(str)

df = df.dropna(subset=['clean_text'])
df['clean_text'] = df['clean_text'].astype(str)
docs = df['clean_text'].tolist()

# 1. Embedding model mạnh hơn
emb_model = SentenceTransformer("all-mpnet-base-v2")

# 2. Vectorizer với custom stop-words
custom_stops = ["just", "like", "know", "think", "people", "https", "com", "www", "reddit", "things", "really", "actually", "chovy"]
stop_words = list(ENGLISH_STOP_WORDS.union(custom_stops))

vect = TfidfVectorizer(
    stop_words=stop_words,
    ngram_range=(1,1),
    min_df=1,
    max_df=0.9             # loại bỏ từ quá phổ biến (>90% docs)
)

# 3. UMAP & HDBSCAN tuning
umap_model = UMAP(n_neighbors=50, min_dist=0.01, metric="cosine")
hdbscan_model = HDBSCAN(min_cluster_size=50, min_samples=10, metric="euclidean",
                        cluster_selection_epsilon=0.1,
                        prediction_data=True)

# 4. Build BERTopic
topic_model = BERTopic(
    embedding_model=emb_model,
    vectorizer_model=vect,
    umap_model=umap_model,
    hdbscan_model=hdbscan_model,
    calculate_probabilities=True,
    top_n_words=5,
    verbose=True
)

# 1. Train model
topics, probs = topic_model.fit_transform(docs)

# Giảm xuống còn 10 topics chính
topic_model.reduce_topics(
    docs=docs,
    nr_topics='auto'
)

new_topics = topic_model.topics_
new_probs  = topic_model.probabilities_    # if you initialized with calculate_probabilities=True

topics_arr = np.array(new_topics)
probs_arr  = np.array(new_probs)

# Tạo mask chỉ lấy những doc không phải outlier
mask = topics_arr != -1
# Lọc docs, topics, probs tương ứng
clean_topics = topics_arr[mask]
clean_probs  = probs_arr[mask]
clean_docs   = [doc for doc, m in zip(docs, mask) if m]

topic_model.save("bertopic")

print(f"Đã train BERTTopic với topics và lưu xong!")