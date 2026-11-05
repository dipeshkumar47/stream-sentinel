from sentence_transformers import SentenceTransformer

from config import EMBEDDING_MODEL_NAME


class HuggingFaceEmbedder:
    """Wraps SentenceTransformer for in-process log vectorization."""

    def __init__(self, model_name: str = EMBEDDING_MODEL_NAME):
        self._model_name = model_name
        # Load model immediately on construction
        self._model = SentenceTransformer(model_name)

    def embed(self, text: str) -> list[float]:
        return self._model.encode(text).tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return self._model.encode(texts).tolist()