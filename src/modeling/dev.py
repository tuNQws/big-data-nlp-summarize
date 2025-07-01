import pandas as pd
from gensim.corpora import Dictionary
from gensim.models.ldamodel import LdaModel

# 1.1 Đọc data đã clean
df = pd.read_csv("data/reddit_posts_clean.csv", encoding="utf-8")
# Giả sử bạn đã có cột 'clean_text'
texts = df["clean_text"].str.split().tolist()

# 1.2 Xây dictionary & corpus
dictionary = Dictionary(texts)
dictionary.filter_extremes(no_below=20, no_above=0.5)  # giữ từ xuất hiện >=20 docs và <=50% docs
corpus = [dictionary.doc2bow(doc) for doc in texts]

# 1.3 Train LDA
num_topics = 15
lda = LdaModel(
    corpus=corpus,
    id2word=dictionary,
    num_topics=num_topics,
    passes=15,
    random_state=42
)

# 1.4 Lưu model và dictionary
dictionary.save("reddit.dict")
lda.save("reddit_lda.model")

print(f"Đã train LDA với {num_topics} topics và lưu xong!")