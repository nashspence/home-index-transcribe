import json
from pathlib import Path

from .chunk_utils import segments_to_chunk_docs


def migrate_v1_segments(name, document, metadata_dir_path):
    """Move stored segments from document[name] to chunks.json if present."""
    if name not in document or "segments" not in document[name]:
        return None, []

    segments = document[name].pop("segments")
    chunks_path = Path(metadata_dir_path) / "chunks.json"
    chunks_path.parent.mkdir(parents=True, exist_ok=True)
    with open(chunks_path, "w") as file:
        json.dump(segments, file, indent=4)

    chunk_docs = segments_to_chunk_docs(segments, document["id"], document, name)
    return segments, chunk_docs
