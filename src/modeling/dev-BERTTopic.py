import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

# 1.1 Đọc data đã clean
df = pd.read_csv("data/reddit_posts_clean.csv", encoding="utf-8")
df = df.dropna(subset=["clean_text"])
df["clean_text"] = df["clean_text"].astype(str)

df = df.dropna(subset=['clean_text'])
df['clean_text'] = df['clean_text'].astype(str)
docs = df['clean_text'].tolist()

vectorizer_model = CountVectorizer(
    stop_words="english",
    ngram_range=(1, 2),
    min_df=5
)

# 1.2 Xây dictionary & corpus
topic_model = BERTopic(
    vectorizer_model=vectorizer_model, 
    top_n_words=3,
    n_gram_range=(1, 2),
    nr_topics="auto",
    verbose=True
)
topics, probs = topic_model.fit_transform(docs)

topic_model.save("bertopic")

print(f"Đã train BERTTopic với topics và lưu xong!")