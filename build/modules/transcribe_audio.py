import logging
import asyncio

lock = asyncio.Lock()

NAME = "transcribe audio"
DATA_FIELD_NAMES = ["transcribed_audio"]
FILTERABLE_FIELD_NAMES = []
SORTABLE_FIELD_NAMES = []
VERSION = 1

async def get_supported_mime_types():
    return [
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
    ]

whisperx_module = None
async def init():
    global whisperx_module
    import whisperx
    whisperx_module = whisperx
    return

async def cleanup():
    whisperx_module = None
    return

async def get_fields(file_path, mime, info, doc):
    if not info is None and info["version"] == VERSION:
        return
    
    try:
        async with lock:
            audio = whisperx_module.load_audio(file_path)
            model = whisperx_module.load_model("medium", device="cpu", compute_type="int8")
            result = model.transcribe(audio, batch_size=16)
            import gc; gc.collect(); del model
            model_a, metadata = whisperx_module.load_align_model(language_code=result["language"], device="cpu")
            result = whisperx_module.align(result["segments"], model_a, metadata, audio, "cpu", return_char_alignments=False) 
            import gc; gc.collect(); del model_a
            diarize_model = whisperx_module.DiarizationPipeline(use_auth_token="REMOVED", device="cpu")
            diarize_segments = diarize_model(audio)
            import gc; gc.collect(); del diarize_model
            result = whisperx_module.assign_word_speakers(diarize_segments, result)
        
        yield {
            "transcribed_audio": result["segments"]
        }, { "version": VERSION }
    except Exception as e:
        logging.exception(f"transcribe audio exception")
        raise e
