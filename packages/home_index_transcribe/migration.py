import json
from pathlib import Path

from home_index_module import segments_to_chunk_docs, apply_migrations as _apply_migrations

# MIGRATIONS[i] runs the upgrade from version i to i+1.


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


MIGRATIONS = [None, migrate_v1_segments]


def apply_migrations(from_version, name, document, metadata_dir_path, target_version):
    """Delegate to ``home_index_module.apply_migrations`` with module migrations."""

    return _apply_migrations(
        from_version,
        MIGRATIONS,
        name,
        document,
        metadata_dir_path,
        target_version=target_version,
    )
