import asyncio
import json
import logging
import os

NAME = "transcribe_audio"
VERSION = 1
FILTERABLE_FIELD_NAMES = []
SORTABLE_FIELD_NAMES = []

SUPPORTED_MIME_TYPES = {
    "audio/aac", "audio/flac", "audio/mpeg", "audio/mp4", "audio/ogg", 
    "audio/wav", "audio/webm", "audio/x-wav", "audio/x-m4a", "audio/x-ms-wma",
    "audio/x-ms-wax", "audio/x-flac", "audio/x-musepack", "audio/x-opus",
    "audio/x-vorbis", "audio/x-alac", "video/x-msvideo", "video/x-matroska", 
    "video/x-flv", "video/x-m4v", "video/x-mjpeg", "video/quicktime",
    "video/mp4", "video/mpeg", "video/webm", "video/ogg", "video/x-nut",
    "video/x-matroska", "application/x-mpegURL", "application/ogg", 
    "application/vnd.apple.mpegurl", "application/vnd.rn-realmedia",
    "application/vnd.rn-realmedia-vbr", "application/x-pn-realaudio", 
    "video/x-ms-asf", "video/x-ms-wmv", "video/3gpp", "video/3gpp2"
}

whisperx_module = None
async def init():
    global whisperx_module
    import whisperx
    whisperx_module = whisperx
    return

async def cleanup():
    whisperx_module = None
    return

lock = asyncio.Lock()

async def get_fields(file_path, module_save_path, document):
    mime = document["type"]
    if mime not in SUPPORTED_MIME_TYPES:
        return

    version_path = module_save_path / "version.json"
    transcription_path = module_save_path / "transcription.json"

    version_info = None
    if version_path.exists():
        with open(version_path, 'r') as file:
            version_info = json.load(file)

    if version_info and version_info.get("version") == VERSION:
        return

    if version_info:
        for field in version_info.get("added_fields", []):
            document.pop(field, None)
        if transcription_path.exists():
            os.remove(transcription_path)

    try:
        # Lock and transcribe the audio
        async with lock:
            audio = whisperx_module.load_audio(file_path)
            model = whisperx_module.load_model("medium", device="cpu", compute_type="int8")
            result = model.transcribe(audio, batch_size=16)
            import gc; gc.collect(); del model  # Clean up

            model_a, metadata = whisperx_module.load_align_model(language_code=result["language"], device="cpu")
            result = whisperx_module.align(result["segments"], model_a, metadata, audio, "cpu", return_char_alignments=False) 
            import gc; gc.collect(); del model_a  # Clean up

            diarize_model = whisperx_module.DiarizationPipeline(use_auth_token="REMOVED", device="cpu")
            diarize_segments = diarize_model(audio)
            import gc; gc.collect(); del diarize_model  # Clean up

            result = whisperx_module.assign_word_speakers(diarize_segments, result)

        with open(transcription_path, 'w') as file:
            json.dump(result, file, indent=4)

        plain_text = " ".join([segment["text"].strip() for segment in result["segments"]]).strip()
        plain_text = " ".join(plain_text.split())
        document["transcribed_audio"] = plain_text

        with open(version_path, 'w') as file:
            json.dump({"version": VERSION, "added_fields": ["transcribed_audio"]}, file, indent=4)

        yield document

    except Exception as e:
        logging.exception("Transcribe audio exception")
        raise e
