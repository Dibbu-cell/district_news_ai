import numpy as np
from sklearn.cluster import DBSCAN


def cluster_news(embeddings):

    X = np.array(embeddings)

    model = DBSCAN(
        eps=0.4,
        min_samples=3,
        metric="cosine"
    )

    labels = model.fit_predict(X)

    return labels