import whisperx
import logging

NAME = "transcribe audio"
FIELD_NAME = "transcribed_audio_version"
DATA_FIELD_NAMES = ["transcribed_audio"]
MAX_WORKERS = 1
VERSION = 1

def does_support_mime(mime):
    return mime.startswith("audio/") or mime.startswith("video/")

async def init():
    return

async def cleanup():
    return

async def get_fields(file_path, doc):
    try:
        audio = whisperx.load_audio(file_path)
        model = whisperx.load_model("medium", device="cpu", compute_type="int8")
        result = model.transcribe(audio, batch_size=16)
        import gc; gc.collect(); del model
        model_a, metadata = whisperx.load_align_model(language_code=result["language"], device="cpu")
        result = whisperx.align(result["segments"], model_a, metadata, audio, "cpu", return_char_alignments=False) 
        import gc; gc.collect(); del model_
        diarize_model = whisperx.DiarizationPipeline(use_auth_token="REMOVED", device="cpu")
        diarize_segments = diarize_model(audio)
        import gc; gc.collect(); del diarize_model
        result = whisperx.assign_word_speakers(diarize_segments, result)
        yield {
            "transcribed_audio": result["segments"]
        }
    except Exception as e:
        logging.exception(f"transcribe audio exception")
        raise e
