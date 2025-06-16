import os
from datetime import datetime, timedelta

from langchain_core.documents import Document
from langchain_text_splitters import TokenTextSplitter

BASE_TIMESTAMP_PATH = os.environ.get("BASE_TIMESTAMP_PATH")


def _get_base_timestamp(document):
    """Return a datetime parsed from ``document`` using ``BASE_TIMESTAMP_PATH``."""
    if not BASE_TIMESTAMP_PATH or document is None:
        return None

    value = document
    for part in BASE_TIMESTAMP_PATH.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None

    try:
        return datetime.fromtimestamp(float(value))
    except Exception:
        return None


def segments_to_chunk_docs(segments, file_id, document=None, module_name="chunk"):
    """Convert raw whisper segments to chunk documents.

    ``module_name`` is included in the generated chunk identifier so chunks can
    be deterministically addressed in Meilisearch.
    """
    docs = []

    base_ts = _get_base_timestamp(document)

    for idx, segment in enumerate(segments):
        text = segment.get("text")
        if not text:
            continue

        if base_ts:
            ts = base_ts + timedelta(seconds=float(segment.get("start", 0)))
            timestamp = ts.strftime("[ts: %Y-%m-%d %H:%M]")
            text = f"{timestamp}\n{text}"

        docs.append(
            {
                "id": f"{module_name}_{file_id}_{idx}",
                "file_id": file_id,
                "text": text,
                "start": segment.get("start"),
                "end": segment.get("end"),
            }
        )

    return docs


def split_chunk_docs(chunk_docs, model="intfloat/e5-small-v2", tokens_per_chunk=450, chunk_overlap=50):
    """Return ``chunk_docs`` split by tokens using ``langchain`` utilities."""

    from transformers import AutoTokenizer

    docs = []
    for d in chunk_docs:
        d = d.copy()
        text = d.pop("text")
        docs.append(Document(page_content=text, metadata=d))

    hf_tok = AutoTokenizer.from_pretrained(model)
    splitter = TokenTextSplitter.from_huggingface_tokenizer(
        hf_tok,
        chunk_size=tokens_per_chunk,
        chunk_overlap=chunk_overlap,
    )

    split_docs = splitter.split_documents(docs)

    # LangChain's ``split_documents`` copies each ``Document``'s ``metadata``
    # to every resulting chunk. This preserves our ``id`` field so we can append
    # an index while keeping the base identifier intact.

    counts = {}
    result = []
    for doc in split_docs:
        base_id = doc.metadata.get("id")
        n = counts.get(base_id, 0)
        counts[base_id] = n + 1
        result.append(
            {
                "id": f"{base_id}_{n}" if n else base_id,
                "file_id": doc.metadata.get("file_id"),
                "text": doc.page_content,
                "start": doc.metadata.get("start"),
                "end": doc.metadata.get("end"),
            }
        )

    return result
