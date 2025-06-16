import logging
import os
import json
from xmlrpc.server import SimpleXMLRPCServer
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timedelta

from langchain_core.documents import Document
from langchain_text_splitters import TokenTextSplitter


def setup_debugger():
    """Enable debugpy debugging when DEBUG environment variable is true."""
    if str(os.environ.get("DEBUG", "False")) == "True":
        import debugpy
        debugpy.listen(("0.0.0.0", 5678))
        if str(os.environ.get("WAIT_FOR_DEBUGPY_CLIENT", "False")) == "True":
            print("Waiting for debugger to attach...")
            debugpy.wait_for_client()
            print("Debugger attached.")
            debugpy.breakpoint()


setup_debugger()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 9000))
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")
METADATA_DIRECTORY = Path(os.environ.get("METADATA_DIRECTORY", "/files/metadata"))
FILES_DIRECTORY = Path(os.environ.get("FILES_DIRECTORY", "/files"))
BY_ID_DIRECTORY = Path(os.environ.get("BY_ID_DIRECTORY", str(METADATA_DIRECTORY / "by-id")))
BASE_TIMESTAMP_PATH = os.environ.get("BASE_TIMESTAMP_PATH")


def file_path_from_meili_doc(document):
    relpath = next(iter(document["paths"].keys()))
    return Path(FILES_DIRECTORY / relpath)


def metadata_dir_path_from_doc(name, document):
    dir_path = Path(BY_ID_DIRECTORY / document["id"] / name)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def read_json(path):
    path = Path(path)
    with open(path, "r") as file:
        return json.load(file)


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as file:
        json.dump(data, file, indent=4)


def load_version(metadata_dir_path):
    version_path = Path(metadata_dir_path) / "version.json"
    if version_path.exists():
        return read_json(version_path)
    return None


def save_version(metadata_dir_path, data):
    write_json(Path(metadata_dir_path) / "version.json", data)


def save_version_with_exceptions(metadata_dir_path, version, **exceptions):
    """Save ``version`` metadata alongside any exception details."""
    data = {"version": version}
    for key, exc in exceptions.items():
        if exc is not None:
            data[key] = str(exc)
            # also store generic 'exception' if not already present
            if "exception" not in data:
                data["exception"] = str(exc)
    save_version(metadata_dir_path, data)


def apply_migrations(from_version, migrations, *args, target_version):
    """Run one migration step from ``from_version`` toward ``target_version``."""
    if from_version >= target_version:
        return None, [], from_version

    if from_version >= len(migrations) or migrations[from_version] is None:
        return None, [], from_version

    migration = migrations[from_version]
    segments, docs = migration(*args)
    chunk_docs = docs if docs else []
    return segments, chunk_docs, from_version + 1


def apply_migrations_if_needed(metadata_dir_path, migrations, *args, target_version):
    """Load version info and apply pending migrations until up to date."""
    version_info = load_version(metadata_dir_path) or {}
    current = version_info.get("version", 0)
    all_chunk_docs = []
    segments = None
    while current < target_version:
        segs, docs, new_ver = apply_migrations(
            current, migrations, *args, target_version=target_version
        )
        if new_ver == current:
            break
        current = new_ver
        if segs is not None:
            segments = segs
        all_chunk_docs.extend(docs)
    if current != version_info.get("version"):
        save_version(metadata_dir_path, {"version": current})
    return segments, all_chunk_docs, current


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
    """Convert raw segments to chunk documents with consistent IDs."""
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


@contextmanager
def log_to_file_and_stdout(file_path):
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(LOGGING_LEVEL)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOGGING_LEVEL)
    stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(stream_formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    try:
        yield
    finally:
        root_logger.removeHandler(file_handler)
        file_handler.close()
        root_logger.removeHandler(stream_handler)


def run_server(name, hello_fn, check_fn, run_fn, load_fn=None, unload_fn=None):
    """Run an XML-RPC server exposing common module hooks."""

    class Handler:
        def hello(self):
            logging.info("hello")
            return json.dumps(hello_fn())

        def check(self, docs):
            response = set()
            for document in json.loads(docs):
                file_path = file_path_from_meili_doc(document)
                try:
                    metadata_dir_path = metadata_dir_path_from_doc(name, document)
                    with log_to_file_and_stdout(metadata_dir_path / "log.txt"):
                        if check_fn(file_path, document, metadata_dir_path):
                            response.add(document["id"])
                except Exception:
                    logging.exception(f'failed to check "{file_path}"')
            return json.dumps(list(response))

        def load(self):
            logging.info("load")
            if load_fn:
                load_fn()

        def run(self, document_json):
            document = json.loads(document_json)
            file_path = file_path_from_meili_doc(document)
            metadata_dir_path = metadata_dir_path_from_doc(name, document)
            with log_to_file_and_stdout(metadata_dir_path / "log.txt"):
                result = run_fn(file_path, document, metadata_dir_path)
            return json.dumps(result)

        def unload(self):
            logging.info("unload")
            if unload_fn:
                unload_fn()

    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True)
    server.register_instance(Handler())
    print(f"Server running at {server.server_address}")
    server.serve_forever()
