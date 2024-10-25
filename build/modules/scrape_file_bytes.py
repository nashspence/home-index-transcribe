import aiohttp
import asyncio
import datetime
import exiftool
import ffmpeg
import flatdict
import jmespath
import json
import logging
import pymediainfo
import re
import subprocess

from pathlib import Path
from tika import parser as tika_parser, config as tika_config

NAME = "scrape file bytes"
DATA_FIELD_NAMES = [
    'altitude',
    'audio_bit_depth',
    'audio_bit_rate',
    'audio_channels',
    'audio_codec',
    'audio_sample_rate',
    'camera_lens_make',
    'camera_lens_model',
    'creation_day',
    'creation_hour',
    'creation_microsecond',
    'creation_minute',
    'creation_month',
    'creation_second',
    'creation_timestamp',
    'creation_timezone_name',
    'creation_timezone_offset_minutes',
    'creation_year',
    'device_make',
    'device_model',
    'duration',
    'embed_jpgfromraw_img',
    'embed_preview_img',
    'embed_thumbnail_img',
    'height',
    'image_byte_ranges',
    'latitude',
    'longitude',
    'video_bit_rate',
    'video_codec',
    'video_frame_rate',
    'width'
]
FILTERABLE_FIELD_NAMES = [
    'altitude',
    'audio_bit_depth',
    'audio_bit_rate',
    'audio_channels',
    'audio_codec',
    'audio_sample_rate',
    'camera_lens_make',
    'camera_lens_model',
    'creation_day',
    'creation_hour',
    'creation_microsecond',
    'creation_minute',
    'creation_month',
    'creation_second',
    'creation_timestamp',
    'creation_timezone_name',
    'creation_timezone_offset_minutes',
    'creation_year',
    'device_make',
    'device_model',
    'duration',
    'height',
    'latitude',
    'longitude'
    'video_bit_rate',
    'video_codec',
    'video_frame_rate',
    'width'
]
SORTABLE_FIELD_NAMES = [
    'duration',
    'creation_timestamp'
]
VERSION = 1

AUDIO_MIME_TYPES = {
    "audio/aac", "audio/flac", "audio/mpeg", "audio/mp4", "audio/ogg", 
    "audio/wav", "audio/webm", "audio/x-wav", "audio/x-m4a", "audio/x-ms-wma",
    "audio/x-ms-wax", "audio/x-flac", "audio/x-musepack", "audio/x-opus",
    "audio/x-vorbis", "audio/x-alac", "application/x-pn-realaudio"
}

VIDEO_MIME_TYPES = {
    "video/x-msvideo", "video/x-matroska", "video/x-flv", "video/x-m4v", 
    "video/x-mjpeg", "video/quicktime", "video/mp4", "video/mpeg", "video/webm", 
    "video/ogg", "video/x-nut", "video/x-matroska", "video/x-ms-asf", 
    "video/x-ms-wmv", "video/3gpp", "video/3gpp2", "application/x-mpegURL", 
    "application/vnd.apple.mpegurl", "application/vnd.rn-realmedia", 
    "application/vnd.rn-realmedia-vbr"
}

IMAGE_MIME_TYPES = {
    "image/jpeg", "image/pjpeg", "image/png", "image/gif", "image/bmp", 
    "image/webp", "image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon", 
    "image/heif", "image/heic", "image/tiff", "image/x-tiff", "image/avif", 
    "image/x-adobe-dng", "image/x-canon-cr2", "image/x-canon-crw", 
    "image/x-nikon-nef", "image/x-nikon-nrw", "image/x-sony-arw", 
    "image/x-sony-sr2", "image/x-sony-srf", "image/x-fuji-raf", 
    "image/x-panasonic-raw", "image/x-panasonic-rw2", "image/x-olympus-orf", 
    "image/x-pentax-pef", "image/x-sigma-x3f", "image/x-leica-rwl", 
    "image/x-epson-erf", "image/x-kodak-dcr", "image/x-kodak-k25", 
    "image/x-kodak-kdc", "image/x-minolta-mrw", "image/x-mamiya-mef", 
    "image/x-hasselblad-3fr", "image/x-hasselblad-fff", "image/x-phaseone-iiq", 
    "image/x-sraw"
}

TIKA_SUPPORTED_MIME_TYPES = None # lazy - requested in get_supported_mime_types

ARCHIVE_MIME_TYPES = {
    'application/zip', 'application/x-tar', 'application/gzip',
    'application/x-bzip2', 'application/x-7z-compressed', 'application/x-rar-compressed',
    'application/vnd.rar', 'application/x-ace-compressed', 'application/x-apple-diskimage',
    'application/x-xz', 'application/x-lzip', 'application/x-lzma'
}

def extract_location_data(data):
    def from_iso6709():
        iso6709_pattern = r'(?P<latitude>[+-]\d{2}(?:\.\d+)?)(?P<longitude>[+-]\d{3}(?:\.\d+)?)(?P<altitude>[+-]\d{2,3}(?:\.\d+)?)?(?:CRS[A-Za-z0-9:_\-]+)?\/?$'
        pattern = re.compile(iso6709_pattern)
        for value in data.get('iso6709', []):
            match = pattern.match(value)
            if match:
                result = {
                    'latitude': float(match.group('latitude')),
                    'longitude': float(match.group('longitude'))
                }
                if match.group('altitude'):
                    result['altitude'] = float(match.group('altitude'))
                return result
        return None

    def from_gps_coordinates():
        for coord in data.get('gps_coordinates', []):
            try:
                gps_coords = coord.strip().split()
                if len(gps_coords) < 2:
                    continue
                result = {
                    'latitude': float(gps_coords[0]),
                    'longitude': float(gps_coords[1])
                }
                if len(gps_coords) == 3:
                    result['altitude'] = float(gps_coords[2])
                return result
            except ValueError as e:
                logging.exception(e)
                continue
        return None

    def from_gps_position():
        for position in data.get('gps_position', []):
            try:
                coords = position.strip().split()
                if len(coords) < 2:
                    continue
                result = {
                    'latitude': float(coords[0]),
                    'longitude': float(coords[1])
                }
                altitude_list = data.get('altitude', [])
                altitude_ref_list = data.get('altitude_ref', [])
                if altitude_list:
                    altitude = float(str(altitude_list[0]).strip())
                    if altitude_ref_list:
                        altitude_ref = str(altitude_ref_list[0]).strip()
                        if altitude_ref != '0':
                            altitude = -altitude
                    result['altitude'] = altitude
                return result
            except Exception as e:
                logging.exception(e)
                continue
        return None

    def from_lat_long():
        latitude_list = data.get('latitude', [])
        longitude_list = data.get('longitude', [])
        
        if not latitude_list or not longitude_list:
            return None
        
        try:
            latitude = float(latitude_list[0].strip())
            longitude = float(longitude_list[0].strip())
            latitude_ref = data.get('latitude_ref', ['N'])[0].strip()
            longitude_ref = data.get('longitude_ref', ['E'])[0].strip()
            latitude = latitude if latitude_ref == 'N' else -latitude
            longitude = longitude if longitude_ref == 'E' else -longitude

            result = {
                'latitude': latitude,
                'longitude': longitude
            }

            altitude_list = data.get('altitude', [])
            altitude_ref_list = data.get('altitude_ref', [])
            if altitude_list:
                altitude = float(altitude_list[0].strip())
                if altitude_ref_list:
                    altitude_ref = altitude_ref_list[0].strip()
                    if altitude_ref != '0':
                        altitude = -altitude
                result['altitude'] = altitude

            return result
        except Exception as e:
            logging.exception("Failed to parse latitude or longitude values.")
            return None


    for extractor in [from_iso6709, from_gps_coordinates, from_gps_position, from_lat_long]:
        result = extractor()
        if result:
            return result

    return None

LOCATION = ({
    'iso6709': [
        'ffprobe.format.tags.location',
        'ffprobe.format.tags."location-eng"',
        'ffprobe.format.tags.com.apple.quicktime.location.ISO6709',
        'libmediainfo.tracks.General[0].comapplequicktimelocationiso6709',
        'libmediainfo.tracks.General[0].xyz'
    ],
    'gps_coordinates': [
        'exiftool."QuickTime:GPSCoordinates"'
    ],
    'altitude': [
        'exiftool."Composite:GPSAltitude"',
        'exiftool.GPSAltitude'
    ],
    'altitude_ref': [
        'exiftool."Composite:GPSAltitudeRef"',
        'exiftool.GPSAltitudeRef'
    ],
    'gps_position': [
        'exiftool."Composite:GPSPosition"'
    ],
    'latitude': [
        'exiftool."Composite:GPSLatitude"',
        'exiftool.GPSLatitude'
    ],
    'longitude': [
        'exiftool."Composite:GPSLongitude"',
        'exiftool.GPSLongitude'
    ],
    'latitude_ref': [
        'exiftool."Composite:GPSLatitudeRef"',
        'exiftool.GPSLatitudeRef'
    ],
    'longitude_ref': [
        'exiftool."Composite:GPSLongitudeRef"',
        'exiftool.GPSLongitudeRef'
    ],
}, extract_location_data)

DATE_REGEX_PATTERN = r'''(?x)
    ^
    (?P<year>\d{4})
    (?:
        (?P<date_sep>[-:]?)
        (?P<month>\d{2})
        (?:
            (?P=date_sep)
            (?P<day>\d{2})
        )?
    )?
    (?:
        (?P<datetime_sep>[T\s]?)
        (?P<hour>\d{2})
        (?:
            (?:
                (?P<time_sep>[:]?)
                (?P<minute>\d{2})
                (?:
                    (?P=time_sep)
                    (?P<second>\d{2})
                    (?:
                        \.(?P<microsecond>\d{1,6})
                    )?
                )?
            )?
        )?
    )?
    (?P<tzinfo>
        Z|
        [+-]\d{2}:?\d{2}
    )?
    $'''

date_regex = re.compile(DATE_REGEX_PATTERN, re.VERBOSE)

def extract_timestamp(data):
    parsed_datetimes = []
    
    for key in [
        'iso8601',
        'filepath.datetime',
        'datetime_with_microsecond',
        'datetime_with_offset',
        'datetime_without_offset'
    ]:
        found_values = data.get(key, [])  
        for value in found_values:
            try:
                value = value.strip()
                match = date_regex.match(value)
                if match:
                    parsed_components = match.groupdict()
                    dt_kwargs = {}
                    for k in ['year', 'month', 'day', 'hour', 'minute', 'second']:
                        v = parsed_components.get(k)
                        if v is not None:
                            dt_kwargs[k] = int(v)
                    dt_kwargs.setdefault('hour', 0)
                    dt_kwargs.setdefault('minute', 0)
                    dt_kwargs.setdefault('second', 0)
                    if parsed_components.get('microsecond'):
                        microsecond = (parsed_components['microsecond'] + '000000')[:6]
                        dt_kwargs['microsecond'] = int(microsecond)
                    else:
                        dt_kwargs['microsecond'] = 0
                    offset = parsed_components.get('tzinfo')
                    if not offset and key == 'datetime_without_offset':
                        possible_offsets = data.get("offset_time", [])
                        if len(possible_offsets) > 0:
                            offset = possible_offsets[0]
                    if offset == 'Z':
                        dt_kwargs['tzinfo'] = datetime.timezone.utc
                    elif offset:
                        sign = 1 if offset[0] == '+' else -1
                        offset_clean = offset.replace(':', '')
                        hours = int(offset_clean[1:3])
                        minutes = int(offset_clean[3:5])
                        total_minutes = sign * (hours * 60 + minutes)
                        dt_kwargs['tzinfo'] = datetime.timezone(datetime.timedelta(minutes=total_minutes))
                    else:
                        dt_kwargs['tzinfo'] = None
                    dt = datetime.datetime(
                        dt_kwargs.get("year"),  
                        dt_kwargs.get("month", 1),
                        dt_kwargs.get("day", 1),
                        dt_kwargs.get("hour", 0), 
                        dt_kwargs.get("minute", 0),
                        dt_kwargs.get("second", 0), 
                        dt_kwargs.get("microsecond", 0),
                        dt_kwargs.get("tzinfo", None), 
                    )
                    datetime_components = {'year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond', 'tzinfo'}
                    components = set(k for k, v in parsed_components.items() if v and k in datetime_components)
                    parsed_datetimes.append((dt, components))
            except Exception as e:
                logging.exception(e)
                pass
                
    ctime_list = data.get('fs.ctime', [])
    ctime = ctime_list[0] if ctime_list else None
    
    timestamp_data = {}
    if parsed_datetimes:
        def compare_by_components(dt_tuple):
            dt, components = dt_tuple
            component_order = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond']
            component_values = []
            for comp in component_order:
                if comp in components:
                    component_values.append(getattr(dt, comp))
                else:
                    component_values.append(float('inf'))
            return (component_values, -len(components))
        best_dt, components = min(parsed_datetimes, key=compare_by_components)
        if best_dt.tzinfo is None:
            best_dt = best_dt.replace(tzinfo=datetime.timezone.utc)
        if ctime:
            try:
                timestamp = float(ctime)
                ctime_dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
                components_match = True
                best_dt_utc = best_dt.astimezone(datetime.timezone.utc)
                for comp in components:
                    if comp != 'timezone_offset':
                        if getattr(best_dt_utc, comp) != getattr(ctime_dt, comp):
                            components_match = False
                            break
                if components_match:
                    tzinfo = getattr(best_dt, 'tzinfo')
                    if tzinfo:
                        ctime_dt = ctime_dt.astimezone(tzinfo)
                    best_dt = ctime_dt
            except Exception as e:
                logging.exception(e)
                pass
        timestamp_data = {
            f"creation_{comp}": getattr(best_dt, comp)
            for comp in components if comp != 'tzinfo'
        }
        if 'tzinfo' in components:
            if best_dt.utcoffset() is not None:
                timestamp_data['creation_timezone_offset_minutes'] = int(best_dt.utcoffset().total_seconds() / 60)
            else:
                timestamp_data['creation_timezone_offset_minutes'] = None
            timestamp_data['creation_timezone_name'] = best_dt.tzname()
        timestamp_data['creation_timestamp'] = best_dt.timestamp()
    else:
        if ctime:
            try:
                timestamp = float(ctime)
                timestamp_data['creation_timestamp'] = timestamp
            except Exception as e:
                logging.exception(e)
                pass
    return timestamp_data

TIMESTAMP = ({
    'iso8601': [
        'ffprobe.format.tags.creation_time',
        'ffprobe.streams.video[0].tags.creation_time',
        'ffprobe.streams.audio[0].tags.creation_time',
        'ffprobe.format.tags.com.apple.quicktime.creationdate'
    ],
    'datetime_with_microsecond': [
        'exiftool."Composite:SubSecCreateDate"',
        'exiftool."Composite:SubSecDateTimeOriginal"'
    ],
    'datetime_with_offset': [
        'exiftool."XML:CreationDateValue"',
        'exiftool."QuickTime:CreationDate"'
    ],
    'datetime_without_offset': [
        'exiftool."EXIF:CreateDate"',
        'exiftool."EXIF:DateTimeOriginal"',
        'exiftool."QuickTime:CreateDate"',
        'exiftool."QuickTime:MediaCreateDate"',
        'exiftool."QuickTime:TrackCreateDate"'
    ],
    'offset_time': [
        'exiftool."EXIF:OffsetTime"',
        'exiftool."EXIF:OffsetTimeOriginal"',
        'exiftool."EXIF:OffsetTimeDigitized"'
    ],
    'fs.ctime': [
        'fs.ctime'
    ],
    'filepath.datetime': [
        'filepath.datetime'
    ],
}, extract_timestamp)

AUDIO_BIT_RATE = ({
    'audio_bit_rate': [
        'ffprobe.format.bit_rate',
        'libmediainfo.tracks.General[0].overall_bit_rate',
        'libmediainfo.tracks.Audio[0].bit_rate',
        'ffprobe.streams.audio[0].bit_rate'
    ]
}, lambda d: {'audio_bit_rate': int(str(d['audio_bit_rate'][0]).strip().lower())} if d['audio_bit_rate'] else None)

VIDEO_AUDIO_BIT_RATE = ({
    'video_bit_rate': [
        'libmediainfo.tracks.Audio[0].bit_rate',
        'ffprobe.streams.audio[0].bit_rate'
    ]
}, lambda d: {'video_bit_rate': int(str(d['video_bit_rate'][0]).strip().lower())} if d['video_bit_rate'] else None)

AUDIO_CHANNELS = ({
    'audio_channels': [
        'ffprobe.streams.audio[0].channels',
        'libmediainfo.tracks.Audio[0].channel_s',
        'exiftool."XML:AudioFormatNumOfChannel"',
        'exiftool."QuickTime:AudioChannels"'
    ]
}, lambda d: {'audio_channels': int(str(d['audio_channels'][0]).strip().lower())} if d['audio_channels'] else None)

AUDIO_DURATION = ({
    'duration': [
        'ffprobe.format.duration',
        'ffprobe.streams.audio[0].duration',
        'exiftool."XML:DurationValue"'
    ]
}, lambda d: {'duration': float(str(d['duration'][0]).strip().lower())} if d['duration'] else None)

AUDIO_CODEC = ({
    'audio_codec': [
        'ffprobe.streams.audio[0].codec_name',
        'libmediainfo.tracks.Audio[0].format',
        'exiftool."QuickTime:AudioFormat"'
    ]
}, lambda d: {'audio_codec': str(d['audio_codec'][0]).strip().lower()} if d['audio_codec'] else None)

AUDIO_SAMPLE_RATE = ({
    'audio_sample_rate': [
        'libmediainfo.tracks.Audio[0].sampling_rate',
        'ffprobe.streams.audio[0].sample_rate',
        'exiftool."QuickTime:AudioSampleRate"'
    ]
}, lambda d: {'audio_sample_rate': int(str(d['audio_sample_rate'][0]).strip().lower())} if d['audio_sample_rate'] else None)

DEVICE_MAKE = ({
    'device_make': [
        'exiftool."EXIF:Make"',
        'exiftool."XML:DeviceManufacturer"',
        'exiftool."QuickTime:Make"',
        'exiftool."QuickTime:AndroidManufacturer"',
        'ffprobe.format.tags.com.android.manufacturer',
        'libmediainfo.tracks.General[0].comandroidmanufacturer',
        'exiftool."ICC_Profile:DeviceManufacturer"'
    ]
}, lambda d: {'device_make': str(d['device_make'][0]).strip().lower()} if d['device_make'] else None)

DEVICE_MODEL = ({
    'device_model': [
        'exiftool."EXIF:Model"',
        'exiftool."XML:DeviceModelName"',
        'exiftool."QuickTime:Model"',
        'exiftool."QuickTime:AndroidModel"',
        'ffprobe.format.tags.com.android.model',
        'libmediainfo.tracks.General[0].comandroidmodel',
        'exiftool."ICC_Profile:DeviceModel"'
    ]
}, lambda d: {'device_model': str(d['device_model'][0]).strip().lower()} if d['device_model'] else None)

EMBEDDED_PREVIEW_IMAGE = ({
    'embed_preview_img_start': [
        'exiftool."EXIF:PreviewImageStart"'
    ],
    'embed_preview_img_length': [
        'exiftool."EXIF:PreviewImageLength"'
    ]
}, lambda d: {'embed_preview_img': f"bytes={int(str(d['embed_preview_img_start'][0]).strip())}-{int(str(d['embed_preview_img_length'][0]).strip())}"} if d['embed_preview_img_start'] and d['embed_preview_img_length'] else None)

EMBEDDED_THUMBNAIL_IMAGE = ({
    'embed_thumbnail_img_start': [
        'exiftool."EXIF:ThumbnailOffset"'
    ],
    'embed_thumbnail_img_length': [
        'exiftool."EXIF:ThumbnailLength"'
    ]
}, lambda d: {'embed_thumbnail_img': f"bytes={int(str(d['embed_thumbnail_img_start'][0]).strip())}-{int(str(d['embed_thumbnail_img_length'][0]).strip())}"} if d['embed_thumbnail_img_start'] and d['embed_thumbnail_img_length'] else None)

EMBEDDED_JPGFROMRAW_IMAGE = ({
    'embed_jpgfromraw_img_start': [
        'exiftool."EXIF:JpgFromRawStart"'
    ],
    'embed_jpgfromraw_img_length': [
        'exiftool."EXIF:JpgFromRawLength"'
    ]
}, lambda d: {'embed_jpgfromraw_img': f"bytes={int(str(d['embed_jpgfromraw_img_start'][0]).strip())}-{int(str(d['embed_jpgfromraw_img_length'][0]).strip())}"} if d['embed_jpgfromraw_img_start'] and d['embed_jpgfromraw_img_length'] else None)

byte_length_regex = r"\(Binary data (\d+) bytes, use -b option to extract\)"

def calculate_multi_pic_ranges(d):
    image_byte_ranges = []
    try:
        multi_pic_0_start = int(str(d['multi_pic_0_start'][0]).strip())
        multi_pic_0_length = int(str(d['multi_pic_0_length'][0]).strip())
        multi_pic_0_end = multi_pic_0_start + multi_pic_0_length
        image_byte_ranges.append(f"bytes={multi_pic_0_start}-{multi_pic_0_end}")
        current_start = multi_pic_0_end
        for additional_length_data in d['multi_pic_additional_length']:
            match = re.search(byte_length_regex, additional_length_data)
            if match:
                additional_length = int(match.group(1))
                current_end = current_start + additional_length
                image_byte_ranges.append(f"bytes={current_start}-{current_end}")
                current_start = current_end
    except (KeyError, ValueError, IndexError):
        return None
    return {"image_byte_ranges": image_byte_ranges}

MULTIIMAGE = ({
    'multi_pic_0_start': [
        'exiftool."MPF:MPImageStart"'
    ],
    'multi_pic_0_length': [
        'exiftool."MPF:MPImageLength"'
    ],
    'multi_pic_additional_length': [
        'exiftool."MPF:MPImage2"',
        'exiftool."MPF:MPImage3"',
        'exiftool."MPF:MPImage4"',
        'exiftool."MPF:MPImage5"',
        'exiftool."MPF:MPImage6"',
        'exiftool."MPF:MPImage7"',
        'exiftool."MPF:MPImage8"',
        'exiftool."MPF:MPImage9"',
        'exiftool."MPF:MPImage10"',
        'exiftool."MPF:MPImage11"',
        'exiftool."MPF:MPImage12"',
        'exiftool."MPF:MPImage13"',
        'exiftool."MPF:MPImage14"',
        'exiftool."MPF:MPImage15"',
        'exiftool."MPF:MPImage16"',
        'exiftool."MPF:MPImage17"',
        'exiftool."MPF:MPImage18"',
        'exiftool."MPF:MPImage19"',
        'exiftool."MPF:MPImage20"',
    ]
}, calculate_multi_pic_ranges)

VIDEO_DURATION = ({
    'duration': [
        'ffprobe.format.duration',
        'ffprobe.streams.video[0].duration',
        'exiftool."XML:DurationValue"'
    ]
}, lambda d: {'duration': float(str(d['duration'][0]).strip())} if d['duration'] else None)

CAMERA_LENS_MAKE = ({
    'camera_lens_make': [
        'exiftool."EXIF:LensMake"'
    ]
}, lambda d: {'camera_lens_make': str(d['camera_lens_make'][0]).lower().strip()} if d['camera_lens_make'] else None)

CAMERA_LENS_MODEL = ({
    'camera_lens_model': [
        'exiftool."EXIF:LensModel"',
        'exiftool."QuickTime:CameraLensModel"'
    ]
}, lambda d: {'camera_lens_model': str(d['camera_lens_model'][0]).lower().strip()} if d['camera_lens_model'] else None)

HEIGHT = ({
    'height': [
        'exiftool."EXIF:ExifImageHeight"',
        'ffprobe.streams.video[0].height',
        'exiftool."QuickTime:ImageHeight"',
        'exiftool."File:ImageHeight"'
    ]
}, lambda d: {'height': int(str(d['height'][0]).strip())} if d['height'] else None)

WIDTH = ({
    'width': [
        'exiftool."EXIF:ExifImageWidth"',
        'ffprobe.streams.video[0].width',
        'exiftool."QuickTime:ImageWidth"',
        'exiftool."File:ImageWidth"'
    ]
}, lambda d: {'width': int(str(d['width'][0]).strip())} if d['width'] else None)

VIDEO_BIT_RATE = ({
    'video_bit_rate': [
        'libmediainfo.tracks.Video[0].bit_rate',
        'ffprobe.streams.video[0].bit_rate'
    ]
}, lambda d: {'video_bit_rate': int(str(d['video_bit_rate'][0]).strip())} if d['video_bit_rate'] else None)

VIDEO_CODEC = ({
    'video_codec': [
        'ffprobe.streams.video[0].codec_name'
    ]
}, lambda d: {'video_codec': str(d['video_codec'][0]).lower().strip()} if d['video_codec'] else None)

VIDEO_FRAME_RATE = ({
    'video_frame_rate': [
        'libmediainfo.tracks.General[0].frame_rate',
        'libmediainfo.tracks.Video[0].frame_rate',
        'ffprobe.streams.video[0].r_frame_rate'
    ]
}, lambda d: {'video_frame_rate': float(str(d['video_frame_rate'][0]).strip())} if d['video_frame_rate'] else None)

TEXT = ({
    'text': [
        'tika.content',
    ]
}, lambda d: {'text': re.sub(r'\n\s*\n+', '\n\n', str(d['text'][0]).strip())} if d['text'] else None)

DESIRED_VIDEO = [
    AUDIO_BIT_RATE,
    AUDIO_CHANNELS,
    AUDIO_CODEC,
    AUDIO_SAMPLE_RATE,
    CAMERA_LENS_MAKE,
    CAMERA_LENS_MODEL,
    DEVICE_MAKE,
    DEVICE_MODEL,
    EMBEDDED_JPGFROMRAW_IMAGE,
    EMBEDDED_PREVIEW_IMAGE,
    EMBEDDED_THUMBNAIL_IMAGE,
    HEIGHT,
    LOCATION,
    MULTIIMAGE,
    TIMESTAMP,
    VIDEO_BIT_RATE,
    VIDEO_CODEC,
    VIDEO_DURATION,
    VIDEO_FRAME_RATE,
    WIDTH
]

DESIRED_AUDIO = [
    AUDIO_BIT_RATE,
    AUDIO_CHANNELS,
    AUDIO_CODEC,
    AUDIO_SAMPLE_RATE,
    DEVICE_MAKE,
    DEVICE_MODEL,
    AUDIO_DURATION,
    LOCATION,
    TIMESTAMP
]

DESIRED_IMAGE = [
    CAMERA_LENS_MAKE,
    CAMERA_LENS_MODEL,
    DEVICE_MAKE,
    DEVICE_MODEL,
    EMBEDDED_JPGFROMRAW_IMAGE,
    EMBEDDED_PREVIEW_IMAGE,
    EMBEDDED_THUMBNAIL_IMAGE,
    HEIGHT,
    LOCATION,
    MULTIIMAGE,
    TIMESTAMP,
    WIDTH
]

DESIRED_OTHER = [
    TIMESTAMP,
    TEXT
]

def jmespath_search_with_fallbacks(data, search_dict):
    field_results = {}
    for field, query_list in search_dict.items():
        result = None
        field_results[field] = []
        for query in query_list:
            try:
                result = jmespath.search(query, data)
                if result is not None:
                    field_results[field].append(result)
            except Exception as e:
                logging.exception(e)
                continue
    return field_results

def jmespath_search_with_shaped_list(data, list):
    results = {}
    for search_dict, parser in list:
        try:
            search_results = jmespath_search_with_fallbacks(data, search_dict)
            if search_results:
                if parser:
                    parsed_results = parser(search_results)
                    if parsed_results:
                        results.update(parsed_results)
                else:
                    for k, v in search_results.items():
                        if len(v) > 0:
                            results[k] = v[0]
        except Exception as e:
            logging.exception(e)
            continue
    return results

def scrape_file_path(filepath):
    pattern = r'''(?x)
        (^|(?<=/))
        (?P<year>\d{4})
        (?:
            [ ._:/-]*
            (?P<month>0[1-9]|1[0-2])
            (?:
                [ ._:/-]*
                (?P<day>0[1-9]|[12]\d|3[01])
                (?:
                    (?:[ ._:/-]*|T)
                    (?P<hour>0[0-9]|1[0-9]|2[0-3])
                    (?:
                        [ ._:/-]*
                        (?P<minute>[0-5]\d)
                        (?:
                            [ ._:/-]*
                            (?P<second>[0-5]\d)
                            (?:
                                [ ._:/-]*
                                (?P<fraction>\d+)
                            )?
                        )?
                    )?
                    (?:
                        [ ._:/-]*
                        (?P<offset>Z|[+-](?:0[0-9]|1[0-9]|2[0-3])[ ._:/-]?[0-5]\d)?
                    )?
                )?
            )?
        )?
    '''
    match = re.search(pattern, filepath)
    if match:
        result = match.group('year')
        if match.group('month'):
            result += f"-{match.group('month')}"
        if match.group('day'):
            result += f"-{match.group('day')}"
        if match.group('hour'):
            result += f"T{match.group('hour')}"
            if match.group('minute'):
                result += f":{match.group('minute')}"
                if match.group('second'):
                    result += f":{match.group('second')}"
                    if match.group('fraction'):
                        result += f".{match.group('fraction')}"
                elif match.group('fraction'):
                    result += f":00.{match.group('fraction')}"
            elif match.group('second'):
                result += f":00:{match.group('second')}"
                if match.group('fraction'):
                    result += f".{match.group('fraction')}"
            if match.group('offset'):
                offset = match.group('offset')
                if offset != 'Z':
                    sign = offset[0]
                    h_and_m = ''.join(c for c in offset[1:] if c.isdigit())
                    if len(h_and_m) == 4:
                        offset = f"{sign}{h_and_m[:2]}:{h_and_m[2:]}"
                    else:
                        offset = f"{sign}{h_and_m}"
                result += offset
        return {"datetime": result}
    return None

def scrape_with_exiftool(file_path):
    try:
        with exiftool.ExifToolHelper() as et:
            return et.get_metadata(file_path)[0]
    except Exception:
        logging.warning(f"scrape file bytes exiftool failed")
        return None

def scrape_with_ffprobe(file_path):
    try:
        metadata = ffmpeg.probe(file_path)
        streams_by_type = {}
        for stream in metadata.get('streams', []):
            stream_type = stream.get('codec_type', 'unknown')
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)
        metadata['streams'] = streams_by_type
        return metadata
    except Exception:
        logging.warning(f"scrape file bytes ffprobe failed")
        return None
    
def scrape_with_libmediainfo(file_path):
    try:
        media_info = pymediainfo.MediaInfo.parse(file_path)
        return {'tracks': {
            'General': [track.to_data() for track in media_info.general_tracks],
            'Video': [track.to_data() for track in media_info.video_tracks],
            'Audio': [track.to_data() for track in media_info.audio_tracks],
            'Image': [track.to_data() for track in media_info.image_tracks],
            'Text': [track.to_data() for track in media_info.text_tracks],
            'Menu': [track.to_data() for track in media_info.menu_tracks]
        }}
    except Exception:
        logging.warning(f"scrape file bytes libmediainfo failed")
        return None
    
def scrape_with_os(file_path):
    try:
        path = Path(file_path)
        stat_info = path.stat()
        return {
            'atime': stat_info.st_atime,
            'ctime': stat_info.st_ctime,
            'dev': stat_info.st_dev,
            'gid': stat_info.st_gid,
            'ino': stat_info.st_ino,
            'mode': stat_info.st_mode,
            'mtime': stat_info.st_mtime,
            'nlink': stat_info.st_nlink,
            'size': stat_info.st_size,
            'uid': stat_info.st_uid,
        }
    except Exception:
        logging.warning(f"scrape file bytes os stat failed")
        return None
                  
tika_server_process = None
log_file = None

def scrape_with_tika(file_path):
    try:
        return tika_parser.from_file(file_path)
    except Exception:
        try:
            wait_for_tika_server()
            return tika_parser.from_file(file_path)
        except:
            logging.warning(f"scrape file bytes tika failed")
            return None

async def wait_for_tika_server(port, timeout=30):
    start_time = datetime.datetime.now()
    while (datetime.datetime.now() - start_time).total_seconds() < timeout:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{port}/tika') as response:
                    if response.status == 200:
                        return True
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(1)
    raise RuntimeError("Tika server did not start in time")

default_key_regex = r"(?i)(offset|start|length|title|author|desc|bit|depth|rate|channels|codec|sample|frame|height|width|dimension|device|make(?!r)|model|manufactur|camera|lens|altitude|orient|rotat|gps|coordinates|latitude|longitude|location|date|(?<!quick)time|zone|offset|iso8601|iso6709|duration)"
def debug(metadata, file_path, result, key_regex=default_key_regex, file_path_regex=None):
    flat_metadata = dict(flatdict.FlatterDict(metadata, delimiter='.'))
    array_fix_pattern = r"\.(\d+)\.(?!$)|\.(\d+)$"
    key_pattern = re.compile(key_regex) if key_regex else None
    file_path_pattern = re.compile(file_path_regex) if file_path_regex else None
    logging.info(f'scrape file bytes DEBUG "{file_path}"')
    if file_path_pattern is None or file_path_pattern.search(file_path):
        for k, v in flat_metadata.items():
            if key_pattern is None or key_pattern.search(k):
                fixed_key = re.sub(array_fix_pattern, lambda match: f"[{match.group(1)}]." if match.group(1) else f"[{match.group(2)}]", k)
                logging.info(f'"{fixed_key}" : "{v}"')
        logging.info(f'scrape file bytes DEBUG selected ------------------------')
        for k, v in result.items():
            logging.info(f'"{k}" : "{v}"')
        logging.info(f'scrape file bytes DEBUG end ------------------------')


async def init():
    global tika_server_process, log_file
    log_file = open('./data/logs/tika_server.log', 'w')
    
    tika_server_process = subprocess.Popen(
        ['java', '-jar', '/tika-server.jar', '-p', '9998'],
        stdout=log_file,
        stderr=log_file
    )
    
    await wait_for_tika_server(9998)

async def cleanup():
    global tika_server_process, log_file
    if tika_server_process:
        tika_server_process.terminate()
        tika_server_process.wait()
    if log_file:
        log_file.close()
        
async def get_supported_mime_types():
    global TIKA_SUPPORTED_MIME_TYPES
    tika_mimes = await asyncio.to_thread(tika_config.getMimeTypes)
    TIKA_SUPPORTED_MIME_TYPES = set(json.loads(tika_mimes))
    return list((TIKA_SUPPORTED_MIME_TYPES | VIDEO_MIME_TYPES | AUDIO_MIME_TYPES | IMAGE_MIME_TYPES) - ARCHIVE_MIME_TYPES)

async def get_fields(file_path, mime, info, doc):
    if info and info.get("file_path") == file_path and info.get("version") == VERSION:
        return

    try:
        futures = {}
        futures["fs"] = asyncio.to_thread(scrape_with_os, file_path)
        futures["filepath"] = asyncio.to_thread(scrape_file_path, file_path)
        if mime in AUDIO_MIME_TYPES | VIDEO_MIME_TYPES | IMAGE_MIME_TYPES:
            futures["exiftool"] = asyncio.to_thread(scrape_with_exiftool, file_path)
        if mime in AUDIO_MIME_TYPES | VIDEO_MIME_TYPES:
            futures["ffprobe"] = asyncio.to_thread(scrape_with_ffprobe, file_path)
            futures["libmediainfo"] = asyncio.to_thread(scrape_with_libmediainfo, file_path)
        if mime in TIKA_SUPPORTED_MIME_TYPES:
            futures["tika"] = asyncio.to_thread(scrape_with_tika, file_path)

        results = await asyncio.gather(*futures.values())
        metadata = {key: result for key, result in zip(futures.keys(), results)}
        
        if mime in VIDEO_MIME_TYPES:
            desired_fields = jmespath_search_with_shaped_list(metadata, DESIRED_VIDEO)
        elif mime in AUDIO_MIME_TYPES:
            desired_fields = jmespath_search_with_shaped_list(metadata, DESIRED_AUDIO)
        elif mime in IMAGE_MIME_TYPES:
            desired_fields = jmespath_search_with_shaped_list(metadata, DESIRED_IMAGE)
        elif mime in TIKA_SUPPORTED_MIME_TYPES:
            desired_fields = jmespath_search_with_shaped_list(metadata, DESIRED_OTHER)
         
        #debug(metadata, file_path, desired_fields, key_regex=None)
        yield desired_fields, {"version": VERSION, "file_path": file_path}
    except Exception:
        logging.exception(f'Failed to scrape file bytes {file_path}')
        raise
