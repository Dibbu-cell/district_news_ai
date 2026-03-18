from sentence_transformers import SentenceTransformer

_model = None


def _get_model():

    global _model

    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")

    return _model


def generate_embedding(text):

    emb = _get_model().encode(text)

    return emb.tolist()


def generate_embeddings(texts, batch_size=64):

    if not texts:
        return []

    embeddings = _get_model().encode(texts, batch_size=batch_size, show_progress_bar=False)

    return [embedding.tolist() for embedding in embeddings]