# region "import"


import logging
import collections
import colorsys
import re
import os
import gc
import torch
from home_index_module import (
    run_server,
    segments_to_chunk_docs,
    load_version,
    apply_migrations_if_needed,
    save_version_with_exceptions,
    write_json,
)
from pathlib import Path
from .migration import MIGRATIONS


# endregion
# region "ass subtitles"


def generate_ass_subtitles(data):
    # Function to generate visually distinct colors
    def generate_colors(n):
        """Generate n visually distinct colors."""
        colors = []
        for i in range(n):
            hue = i / n
            saturation = 0.7  # Fixed saturation
            lightness = 0.5  # Fixed lightness
            rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
            rgb = tuple(int(255 * x) for x in rgb)
            colors.append(rgb)
        return colors

    # Function to adjust lightness
    def adjust_lightness(rgb, factor):
        """Adjust the lightness of an RGB color by a factor (0 to 1)."""
        h, l, s = colorsys.rgb_to_hls(*[x / 255.0 for x in rgb])
        l = max(0, min(1, l * factor))
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return tuple(int(255 * x) for x in (r, g, b))

    # Function to convert RGB to ASS color code
    def rgb_to_ass_color(r, g, b):
        """Convert RGB values to ASS color code in &HBBGGRR format."""
        return f"&H00{b:02X}{g:02X}{r:02X}"

    # Function to sanitize style names
    def sanitize_style_name(name):
        return "".join(c for c in name if c.isalnum() or c == "_")

    # Function to format time in ASS format
    def format_time(seconds):
        """Convert seconds to ASS time format (h:mm:ss.cs)"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}:{minutes:02}:{secs:05.2f}"

    # Collect speaker durations and unique speakers
    speaker_durations = collections.defaultdict(float)
    speakers = set()
    for segment in data["segments"]:
        speaker = segment.get("speaker", "Unknown")
        duration = segment["end"] - segment["start"]
        speaker_durations[speaker] += duration
        speakers.add(speaker)

    # Identify the speaker who speaks the most
    most_speaking_speaker = max(speaker_durations, key=speaker_durations.get)

    # Create speaker list
    speaker_list = sorted(speakers)  # For consistent ordering

    # Generate colors for other speakers
    other_speakers = [s for s in speaker_list if s != most_speaking_speaker]
    colors = generate_colors(len(other_speakers))
    speaker_colors = dict(zip(other_speakers, colors))
    speaker_colors[most_speaking_speaker] = (
        255,
        255,
        255,
    )  # White color for the most frequent speaker

    # Collect style definitions
    style_definitions = []

    speaker_style_names = {}
    for speaker in speaker_list:
        primary_rgb = speaker_colors[speaker]
        primary_color = rgb_to_ass_color(*primary_rgb)
        # Get a darker version for secondary color
        secondary_rgb = adjust_lightness(primary_rgb, 0.7)
        secondary_color = rgb_to_ass_color(*secondary_rgb)
        style_name = sanitize_style_name(speaker)
        speaker_style_names[speaker] = style_name
        # Create the style line
        style_line = f"Style: {style_name},Arial,11,{primary_color},{secondary_color},&H00000000,&H64000000,-1,0,0,0,100,100,0,0,1,1,0,2,10,10,10,1"
        style_definitions.append(style_line)

    dialogue_lines = []

    # Collect dialogue lines
    for segment in data["segments"]:
        start_time = format_time(segment["start"])
        end_time = format_time(segment["end"])
        speaker = segment.get("speaker", "Unknown")
        style = speaker_style_names.get(speaker, "Default")

        # Omit speaker name if it matches 'SPEAKER_{number}'
        if re.match(r"^SPEAKER_\d+$", speaker):
            line_prefix = ""
        else:
            line_prefix = f"{speaker}: "

        # Build the karaoke line without the timestamp
        karaoke_line = f"{{\\k0}}{line_prefix}"
        for word in segment["words"]:
            if "start" in word:
                word_duration = int(
                    (word.get("end", end_time) - word["start"]) * 100
                )  # Convert duration to centiseconds
                karaoke_line += f"{{\\k{word_duration}}}{word['word']} "

        # Add the line to dialogue lines
        dialogue_line = f"Dialogue: 0,{start_time},{end_time},{style},,0,0,0,,{karaoke_line.strip()}"
        dialogue_lines.append(dialogue_line)

    # Construct the entire ASS file content as a single string
    return f"""[Script Info]
Title: Transcribe Module Version
ScriptType: v4.00+
Collisions: Reverse
PlayDepth: 0
Timer: 100.0000

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
{chr(10).join(style_definitions)}

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
{chr(10).join(dialogue_lines)}
"""


# endregion
# region "config"

NAME = os.environ.get("NAME", "transcribe")
VERSION = 2

DEVICE = os.environ.get("DEVICE", "cuda")
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = str(
    os.environ.get("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")
)
BATCH_SIZE = os.environ.get("BATCH_SIZE", 3)
COMPUTE_TYPE = os.environ.get("COMPUTE_TYPE", "int8")
LANGUAGE = os.environ.get("LANGUAGE", "en")
THREADS = os.environ.get("THREADS", 1)
PYTORCH_DOWNLOAD_ROOT = os.environ.get("PYTORCH_DOWNLOAD_ROOT", "/root/.cache/")
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "medium")
PYANNOTE_DIARIZATION_AUTH_TOKEN = os.environ.get("PYANNOTE_DIARIZATION_AUTH_TOKEN")
SUPPORTED_MIME_TYPES = {
    "audio/aac",
    "audio/flac",
    "audio/mpeg",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
    "audio/webm",
    "audio/x-wav",
    "audio/x-m4a",
    "audio/x-ms-wma",
    "audio/x-ms-wax",
    "audio/x-flac",
    "audio/x-musepack",
    "audio/x-opus",
    "audio/x-vorbis",
    "audio/x-alac",
    "video/x-msvideo",
    "video/x-matroska",
    "video/x-flv",
    "video/x-m4v",
    "video/x-mjpeg",
    "video/quicktime",
    "video/mp4",
    "video/mpeg",
    "video/webm",
    "video/ogg",
    "video/x-nut",
    "video/x-matroska",
    "application/x-mpegURL",
    "application/ogg",
    "application/vnd.apple.mpegurl",
    "application/vnd.rn-realmedia",
    "application/vnd.rn-realmedia-vbr",
    "application/x-pn-realaudio",
    "video/x-ms-asf",
    "video/x-ms-wmv",
    "video/3gpp",
    "video/3gpp2",
}


# endregion

# region "hello"


def hello():
    return {
        "name": NAME,
        "version": VERSION,
        "filterable_attributes": [f"{NAME}.text"],
        "sortable_attributes": [],
    }


# endregion
# region "load/unload"


whisperx = None
model = None
align_model = None
align_metadata = None
diarize_model = None


def load():
    global whisperx, model, align_model, align_metadata, diarize_model
    import whisperx as whisperx_module

    whisperx = whisperx_module

    model = whisperx.load_model(
        WHISPER_MODEL,
        DEVICE,
        compute_type=COMPUTE_TYPE,
        language=LANGUAGE,
        threads=THREADS,
        download_root=PYTORCH_DOWNLOAD_ROOT,
    )

    model_a, metadata = whisperx.load_align_model(LANGUAGE, DEVICE)
    align_model = model_a
    align_metadata = metadata

    diarize_model = whisperx.DiarizationPipeline(
        use_auth_token=PYANNOTE_DIARIZATION_AUTH_TOKEN,
        device=DEVICE,
    )


def unload():
    global model, align_model, align_metadata, diarize_model, whisperx
    import torch

    del model
    del align_model
    del align_metadata
    del diarize_model
    del whisperx
    gc.collect()
    torch.cuda.empty_cache()


# endregion
# region "check/run"


def check(file_path, document, metadata_dir_path):
    if not document["type"] in SUPPORTED_MIME_TYPES:
        return False

    version = load_version(metadata_dir_path)
    if version and version.get("version") == VERSION:
        return False

    return True


def run(file_path, document, metadata_dir_path):
    global model, align_model, align_metadata, diarize_model
    logging.info(f"start {file_path}")

    metadata_dir = Path(metadata_dir_path)
    whisperx_path = metadata_dir / "whisperx.json"
    chunks_path = metadata_dir / "chunks.json"
    plaintext_path = metadata_dir / "plaintext.txt"
    subtitle_path = metadata_dir / f"{Path(file_path).stem}.ass"
    prev_version = load_version(metadata_dir_path) or {}
    current_version = prev_version.get("version", 0)
    segments, chunk_docs, new_version = apply_migrations_if_needed(
        metadata_dir_path,
        MIGRATIONS,
        NAME,
        document,
        metadata_dir_path,
        target_version=VERSION,
    )
    if new_version != current_version:
        logging.info("applied migration %s -> %s", current_version, new_version)
        return {"document": document, "chunk_docs": chunk_docs}

    def attempt(batch_size=BATCH_SIZE):
        audio = whisperx.load_audio(file_path)
        result = model.transcribe(audio, language=LANGUAGE, batch_size=batch_size)
        gc.collect()
        torch.cuda.empty_cache()

        result = whisperx.align(
            result["segments"], align_model, align_metadata, audio, DEVICE
        )
        gc.collect()
        torch.cuda.empty_cache()

        diarize_segments = diarize_model(audio)
        result = whisperx.assign_word_speakers(diarize_segments, result)
        gc.collect()
        torch.cuda.empty_cache()

        return result

    result = {}
    whisperx_exception = None
    ass_subtitles_exception = None
    if segments is None:
        try:
            result = attempt()
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            if str(e).startswith("CUDA failed with error out of memory"):
                try:
                    logging.warning("CUDA out of memory. Retrying with BATCH_SIZE=1.")
                    result = attempt(1)
                except FileNotFoundError as e:
                    raise e
                except Exception as e:
                    whisperx_exception = e
                    logging.exception("failed")
            else:
                whisperx_exception = e
                logging.exception("failed")
        segments = result.get("segments", [])
        if segments:
            write_json(whisperx_path, result)

    if segments:
        write_json(chunks_path, segments)

        plaintext = " ".join([segment.get("text", "") for segment in segments])
        document[NAME] = {"text": plaintext}

        with open(plaintext_path, "w") as file:
            file.write(plaintext)

        try:
            ass_subtitles = generate_ass_subtitles({"segments": segments})
            with open(subtitle_path, "w") as file:
                file.write(ass_subtitles)
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            ass_subtitles_exception = e
            logging.exception("ass subtitles failed")

        chunk_docs = segments_to_chunk_docs(segments, document["id"], document, NAME)
    elif not segments:
        chunk_docs = []

    save_version_with_exceptions(
        metadata_dir_path,
        VERSION,
        whisperx_exception=whisperx_exception,
        ass_subtitles_exception=ass_subtitles_exception,
    )

    logging.info("done")
    return {"document": document, "chunk_docs": chunk_docs}


# endregion

if __name__ == "__main__":
    run_server(NAME, hello, check, run, load, unload)
