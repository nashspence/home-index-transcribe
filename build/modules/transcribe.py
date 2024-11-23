import collections
import colorsys
import json
import logging
import re
import warnings
import whisperx

from datetime import datetime, timedelta, timezone
from transcribe_meta import NAME, VERSION

logger = logging

logger1 = logging.getLogger('pytorch_user_warnings')
logger1.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/pytorch_user_warnings.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger1.addHandler(file_handler)
logger1.propagate = False

def warning_to_log(message, category, filename, lineno, file=None, line=None):
    logger1.warning('%s:%s: %s: %s', filename, lineno, category.__name__, message)

warnings.showwarning = warning_to_log
warnings.filterwarnings("ignore", category=UserWarning)

logger2 = logging.getLogger('matplotlib.font_manager')
logger2.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/matplotlib.font_manager.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger2.addHandler(file_handler)
logger2.propagate = False

logger3 = logging.getLogger('speechbrain.utils.quirks')
logger3.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/speechbrain.utils.quirks.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger3.addHandler(file_handler)
logger3.propagate = False

logger4 = logging.getLogger('pytorch_lightning.utilities.migration.utils')
logger4.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/pytorch_lightning.utilities.migration.utils.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger4.addHandler(file_handler)
logger4.propagate = False

logger5 = logging.getLogger('torch')
logger5.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/torch.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger5.addHandler(file_handler)
logger5.propagate = False

def generate_ass_subtitles(data, timestamp, offset_seconds, precision):
    def format_datetime(segment_datetime, precision):
        # Ensure precision is in valid range
        precision = max(0, min(precision, 6))
        
        # Define format levels
        formats = [
            '%Y',                  # 0: Year
            '%Y-%m',               # 1: Year-Month
            '%Y-%m-%d',            # 2: Year-Month-Day
            '%Y-%m-%d %H',         # 3: Year-Month-Day Hour
            '%Y-%m-%d %H:%M',      # 4: Year-Month-Day Hour:Minute
            '%Y-%m-%d %H:%M:%S',   # 5: Year-Month-Day Hour:Minute:Second
            '%Y-%m-%d %H:%M:%S.%f' # 6: Year-Month-Day Hour:Minute:Second.Microsecond
        ]
        
        # Format the datetime
        return segment_datetime.strftime(formats[precision])
    
    # Function to generate visually distinct colors
    def generate_colors(n):
        """Generate n visually distinct colors."""
        colors = []
        for i in range(n):
            hue = i / n
            saturation = 0.7  # Fixed saturation
            lightness = 0.5   # Fixed lightness
            rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
            rgb = tuple(int(255 * x) for x in rgb)
            colors.append(rgb)
        return colors

    # Function to adjust lightness
    def adjust_lightness(rgb, factor):
        """Adjust the lightness of an RGB color by a factor (0 to 1)."""
        h, l, s = colorsys.rgb_to_hls(*[x/255.0 for x in rgb])
        l = max(0, min(1, l * factor))
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return tuple(int(255 * x) for x in (r, g, b))

    # Function to convert RGB to ASS color code
    def rgb_to_ass_color(r, g, b):
        """Convert RGB values to ASS color code in &HBBGGRR format."""
        return f"&H00{b:02X}{g:02X}{r:02X}"

    # Function to sanitize style names
    def sanitize_style_name(name):
        return ''.join(c for c in name if c.isalnum() or c == '_')

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
    speaker_colors[most_speaking_speaker] = (255, 255, 255)  # White color for the most frequent speaker

    # Collect style definitions
    style_definitions = []

    # Define a style for the timestamp
    timestamp_style = "Style: TimestampStyle,Arial,9,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,-1,0,0,0,100,100,0,0,1,1,0,7,10,10,10,1"
    style_definitions.append(timestamp_style)

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

    # Compute the initial timestamp to display
    if timestamp and offset_seconds:
        initial_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc) + timedelta(seconds=offset_seconds)
        formatted_timestamp = format_datetime(initial_datetime, precision)
    else:
        formatted_timestamp = ""

    # Create a dialogue line for the timestamp display
    start_time = format_time(0)
    end_time = format_time(5)
    style = "TimestampStyle"
    marginL = "10"
    marginR = "10"
    marginV = "10"
    alignment = 7  # Top Left
    timestamp_dialogue_line = f"Dialogue: 0,{start_time},{end_time},{style},, {marginL}, {marginR}, {marginV},,{{\\an{alignment}}}{formatted_timestamp}"
    dialogue_lines.append(timestamp_dialogue_line)

    # Collect dialogue lines
    for segment in data["segments"]:
        start_time = format_time(segment["start"])
        end_time = format_time(segment["end"])
        speaker = segment.get("speaker", "Unknown")
        style = speaker_style_names.get(speaker, "Default")

        # Omit speaker name if it matches 'SPEAKER_{number}'
        if re.match(r'^SPEAKER_\d+$', speaker):
            line_prefix = ""
        else:
            line_prefix = f"{speaker}: "

        # Build the karaoke line without the timestamp
        karaoke_line = f"{{\\k0}}{line_prefix}"
        for word in segment["words"]:
            if "start" in word:
                word_duration = int((word.get("end", end_time) - word["start"]) * 100)  # Convert duration to centiseconds
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

model = whisperx.load_model("medium", device="cpu", compute_type="int8", language="en", threads=16, download_root="/data/pytorch")
model_a, metadata = whisperx.load_align_model(language_code="en", device="cpu")
align_model = model_a
align_metadata = metadata
diarize_model = whisperx.DiarizationPipeline(use_auth_token="REMOVED", device="cpu")

def main(file_path, document, doc_db_path, mtime, _logger):
    global logger
    logger = _logger
    logger.info(f'{NAME} start {file_path}')
    
    version_path = doc_db_path / f"{NAME}.json"
    transcription_path = doc_db_path / "transcription.json"
    subtitle_path = doc_db_path / "subtitle.ass"

    audio = whisperx.load_audio(file_path)
    result = model.transcribe(audio, language="en", batch_size=8, num_workers=None) # increasing num_workers breaks whisperX
    result = whisperx.align(result["segments"], align_model, align_metadata, audio, "cpu", return_char_alignments=False) 
    diarize_segments = diarize_model(audio)
    result = whisperx.assign_word_speakers(diarize_segments, result)
    
    if version_path.exists():
        with open(version_path, 'r') as file:
            version_info = json.load(file)
        logger.debug(f'nulling old fields')
        if "added_fields" in version_info:
            for field in version_info["added_fields"]:
                document[field] = None
            
    segments = result.get("segments", [])
    
    if segments:    
        plain_text = " ".join([segment["text"].strip() for segment in result["segments"]]).strip()
        plain_text = " ".join(plain_text.split())
        logger.info(plain_text)             
        document["transcribed_audio"] = plain_text
        
        with open(transcription_path, 'w') as file:
            json.dump(result, file, indent=4)
            
        with open(subtitle_path, 'w') as file:
            offset_seconds = document.get("creation_date_offset_seconds", 0)
            precision = document.get("creation_date_precision", 0)
            file.write(generate_ass_subtitles(result, document["creation_date"], offset_seconds, precision))
    
    with open(version_path, 'w') as file:
        json.dump({"version": VERSION, "added_fields": ["transcribed_audio"]}, file, indent=4)

    logger.info(f'{NAME} done')
    return document