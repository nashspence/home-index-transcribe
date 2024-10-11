from tika import parser, config
import json
import asyncio
import subprocess
import time

log_file = open('./data/logs/tika_server.log', 'w')

tika_server_process = None
tika_server_process = subprocess.Popen(['java', '-jar', '/tika-server.jar', '-p', '9998'],
    stdout=log_file,
    stderr=log_file)
time.sleep(10)
tika_mimes = json.loads(config.getMimeTypes())
time.sleep(10)
tika_server_process.terminate()

NAME = "scrape text"
FIELD_NAME = "scraped_text_version"
DATA_FIELD_NAMES = ["scraped_text"]
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
    global tika_server_process
    tika_server_process = subprocess.Popen(['java', '-jar', '/tika-server.jar', '-p', '9998'],
    stdout=log_file,
    stderr=log_file)
    await asyncio.sleep(10)
    return

async def cleanup():
    tika_server_process.terminate()
    return
    
async def get_fields(file_path, doc):
    parsed = await asyncio.to_thread(parser.from_file, file_path)
    yield { "scraped_text": parsed.get("content", "") }