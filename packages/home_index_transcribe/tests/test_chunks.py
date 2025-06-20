import json
from pathlib import Path
import sys
import types
torch = types.SimpleNamespace(cuda=types.SimpleNamespace(empty_cache=lambda: None))
sys.modules.setdefault("torch", torch)
home_index_module = types.ModuleType("home_index_module")
home_index_module.run_server = lambda *a, **k: None
def _segments_to_chunk_docs(segs, file_id, document=None, module_name="chunk"):
    docs = []
    for idx, s in enumerate(segs):
        docs.append({"id": f"{module_name}_{file_id}_{idx}", "text": s["text"], "start": s["start"], "end": s["end"], "file_id": file_id})
    return docs

def _apply_migrations(from_version, migrations, *args, target_version):
    if from_version >= target_version:
        return None, [], from_version
    if from_version >= len(migrations) or migrations[from_version] is None:
        return None, [], from_version
    segs, docs = migrations[from_version](*args)
    return segs, docs if docs else [], from_version + 1

home_index_module.run_server = lambda *a, **k: None
home_index_module.segments_to_chunk_docs = _segments_to_chunk_docs
home_index_module.apply_migrations = _apply_migrations
def _apply_migrations_if_needed(md, migs, *args, target_version=None):
    current = (_load_version(md) or {}).get("version", 0)
    segs, docs, new = _apply_migrations(current, migs, *args, target_version=target_version)
    if new != current:
        _save_version(md, {"version": new})
    return segs, docs, new

home_index_module.apply_migrations_if_needed = _apply_migrations_if_needed
def _load_version(metadata_dir_path):
    path = Path(metadata_dir_path) / "version.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def _save_version(metadata_dir_path, data):
    path = Path(metadata_dir_path) / "version.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)

def _write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)

home_index_module.load_version = _load_version
home_index_module.save_version = _save_version
home_index_module.save_version_with_exceptions = lambda md, version, **ex: _save_version(md, {"version": version, **{k:str(v) for k,v in ex.items() if v is not None}})
home_index_module.write_json = _write_json
sys.modules.setdefault("home_index_module", home_index_module)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from home_index_transcribe import main as transcribe

def test_migrate_segments_to_chunks(tmp_path):
    segments = [
        {"start": 0.0, "end": 1.0, "text": "hello", "words": []},
        {"start": 1.0, "end": 2.0, "text": "world", "words": []},
    ]
    doc = {
        "id": "file1",
        "type": "audio/ogg",
        "paths": {"file1.opus": 1.0},
        transcribe.NAME: {"segments": segments},
    }
    metadata_dir = tmp_path / transcribe.NAME
    metadata_dir.mkdir(parents=True)
    with open(metadata_dir / "version.json", "w") as f:
        json.dump({"version": 1}, f)
    result = transcribe.run(str(tmp_path / "file1.opus"), doc, metadata_dir)
    with open(metadata_dir / "chunks.json") as f:
        stored = json.load(f)
    assert stored == segments
    assert transcribe.NAME in result["document"]
    assert "segments" not in result["document"][transcribe.NAME]
    assert len(result["chunk_docs"]) == 2
    assert result["chunk_docs"][0]["text"] == "hello"
    assert result["chunk_docs"][0]["start"] == 0.0
    assert result["chunk_docs"][0]["end"] == 1.0
    assert result["chunk_docs"][0]["id"] == f"{transcribe.NAME}_file1_0"

def test_run_generates_chunks(tmp_path, monkeypatch):
    class DummyModel:
        def transcribe(self, audio, language=None, batch_size=None):
            return {"segments": [{"start":0.0,"end":1.0,"text":"hi","words":[]}]} 
    class DummyWhisperx:
        def load_audio(self, fp):
            return None
        def align(self, segs, a, m, audio, device):
            return {"segments": segs}
        def assign_word_speakers(self, diarize_segments, result):
            return result
    transcribe.model = DummyModel()
    transcribe.align_model = None
    transcribe.align_metadata = None
    transcribe.diarize_model = lambda audio: []
    transcribe.whisperx = DummyWhisperx()

    doc = {"id":"file2","type":"audio/ogg","paths":{"f.opus":1.0}}
    metadata_dir = tmp_path / transcribe.NAME
    metadata_dir.mkdir(parents=True)
    result = transcribe.run(str(tmp_path/"f.opus"), doc, metadata_dir)
    with open(metadata_dir / "chunks.json") as f:
        stored = json.load(f)
    assert stored[0]["text"] == "hi"
    assert len(result["chunk_docs"]) == 1
    assert result["document"][transcribe.NAME]["text"] == "hi"
    assert result["chunk_docs"][0]["start"] == 0.0
    assert result["chunk_docs"][0]["end"] == 1.0
    assert result["chunk_docs"][0]["id"] == f"{transcribe.NAME}_file2_0"


def test_multiple_migrations(tmp_path, monkeypatch):
    from home_index_transcribe import migration

    segments = [{"start": 0.0, "end": 1.0, "text": "hi", "words": []}]
    doc = {
        "id": "file3",
        "type": "audio/ogg",
        "paths": {"f.opus": 1.0},
        transcribe.NAME: {"segments": segments},
    }

    def migrate_v2_to_v3(name, document, metadata_dir_path):
        document["migrated"] = True
        return None, []

    monkeypatch.setattr(transcribe, "VERSION", 3)
    monkeypatch.setattr(transcribe, "MIGRATIONS", [None, migration.migrate_v1_segments, migrate_v2_to_v3])
    monkeypatch.setattr(migration, "MIGRATIONS", [None, migration.migrate_v1_segments, migrate_v2_to_v3])

    metadata_dir = tmp_path / transcribe.NAME
    metadata_dir.mkdir(parents=True)
    with open(metadata_dir / "version.json", "w") as f:
        json.dump({"version": 1}, f)

    # First call performs the v1 -> v2 migration and should return chunk docs
    result = transcribe.run(str(tmp_path / "f.opus"), doc, metadata_dir)

    with open(metadata_dir / "version.json") as f:
        stored_version = json.load(f)

    assert stored_version["version"] == 2
    assert "migrated" not in doc
    assert len(result["chunk_docs"]) == 1

    # Second call performs the v2 -> v3 migration
    result = transcribe.run(str(tmp_path / "f.opus"), doc, metadata_dir)

    with open(metadata_dir / "version.json") as f:
        stored_version = json.load(f)

    assert stored_version["version"] == 3
    assert doc["migrated"] is True
    assert result["chunk_docs"] == []
