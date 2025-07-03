import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from sentence_transformers import SentenceTransformer
from umap import UMAP
from hdbscan import HDBSCAN
# topics là mảng int, trong đó -1 = outlier
import numpy as np
from sklearn.decomposition import LatentDirichletAllocation
import pickle

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
    min_df=2,
    max_df=0.9             # loại bỏ từ quá phổ biến (>90% docs)
)

# 3. UMAP & HDBSCAN tuning
umap_model = UMAP(n_neighbors=15, min_dist=0.05, metric="cosine")
hdbscan_model = HDBSCAN(min_cluster_size=15, min_samples=5, metric="euclidean",
                        cluster_selection_epsilon=0.2,
                        prediction_data=True)

# 4. Build BERTopic
topic_model = BERTopic(
    embedding_model=emb_model,
    vectorizer_model=vect,
    # umap_model=umap_model,
    # hdbscan_model=hdbscan_model,
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

# 2. Tách outliers
topics_arr   = np.array(topics)
outlier_idx  = np.where(topics_arr == -1)[0]
outlier_docs = [docs[i] for i in outlier_idx]

# Dùng cùng stop-words hoặc bạn có thể tùy chỉnh riêng cho outliers
tfidf_vect = TfidfVectorizer(
    stop_words=stop_words,   # thừa kế từ vectorizer chính
    ngram_range=(1,2),       # 推荐 1–2 để bắt bigram
    min_df=2,
    max_df=0.95
)

# fit TF-IDF chỉ trên outlier_docs
X_out = tfidf_vect.fit_transform(outlier_docs)

from sklearn.decomposition import LatentDirichletAllocation

n_subtopics = 5
lda = LatentDirichletAllocation(
    n_components=n_subtopics,
    max_iter=10,
    learning_method="batch",
    random_state=42
)
lda.fit(X_out)

# 6. Save LDA model and vectorizer
with open("lda_model.pkl", "wb") as f:
    pickle.dump(lda, f)
with open("lda_vectorizer.pkl", "wb") as f:
    pickle.dump(tfidf_vect, f)

# doc-topic distribution: mỗi hàng là vector dài n_subtopics
doc_topic_dist = lda.transform(X_out)

# chọn sub-topic có xác suất cao nhất
subtopic_labels = doc_topic_dist.argmax(axis=1)

# Lấy danh sách feature names
features = tfidf_vect.get_feature_names_out()

# In top 10 từ mỗi sub-topic
for topic_id, comp in enumerate(lda.components_):
    top_idx = comp.argsort()[:-11:-1]
    top_words = [features[i] for i in top_idx]
    print(f"Sub-topic {topic_id}: {', '.join(top_words)}")
    
final_topics = topics_arr.copy().astype(object)
for idx, sub_lbl in zip(outlier_idx, subtopic_labels):
    final_topics[idx] = f"outlier_sub{sub_lbl}"

# --- 9. Save results ---
df["topic"] = final_topics
df["probability"] = list(probs)  # original probabilities
df.to_csv("reddit_posts_with_topics.csv", index=False, encoding="utf-8")
topic_model.save("bertopic")

print(f"Đã train BERTTopic với topics và lưu xong!")