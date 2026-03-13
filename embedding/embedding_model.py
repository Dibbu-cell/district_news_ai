from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


def generate_embedding(text):

    emb = model.encode(text)

    return emb.tolist()


def generate_embeddings(texts, batch_size=64):

    if not texts:
        return []

    embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=False)

    return [embedding.tolist() for embedding in embeddings]