import json
from pathlib import Path

from .chunk_utils import segments_to_chunk_docs

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
    """Run one migration step and return ``(segments, chunk_docs, new_version)``."""

    if from_version >= target_version:
        return None, [], from_version

    if from_version >= len(MIGRATIONS) or MIGRATIONS[from_version] is None:
        return None, [], from_version

    migration = MIGRATIONS[from_version]
    segments, docs = migration(name, document, metadata_dir_path)
    chunk_docs = docs if docs else []
    return segments, chunk_docs, from_version + 1
