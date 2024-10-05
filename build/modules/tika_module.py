from tika import parser, config
import json
import asyncio

tika_mimes = json.loads(config.getMimeTypes())

NAME = "tika"
FIELD_NAME = "tikaVersion"
DATA_FIELD_NAMES = ["text"]
MAX_WORKERS = 32
VERSION = 1

def does_support_mime(mime):
    archive_mimes = {
        'application/zip', 'application/x-tar', 'application/gzip',
        'application/x-bzip2', 'application/x-7z-compressed', 'application/x-rar-compressed'
    }
    
    if mime in tika_mimes:
        if (mime.startswith("text/") or mime.startswith("application/")) and mime not in archive_mimes:
            return True
    return False    

async def init():
    return

async def cleanup():
    return
    
async def get_fields(file_path, doc):
    parsed = await asyncio.to_thread(parser.from_file, file_path)
    yield { "text": parsed.get("content", "") }