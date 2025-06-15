import os
from datetime import datetime, timedelta


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

