FROM pytorch/pytorch:2.3.1-cuda12.1-cudnn8-runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    attr \
    exiftool \
    ffmpeg \
    file \
    git \
    imagemagick \
    libgomp1 \
    libmagic1 \
    libmediainfo0v5 \
    mediainfo \
    poppler-utils \
    tzdata \
    shared-mime-info \
    && apt-get clean

WORKDIR /app

RUN pip install \
    debugpy \
    easyocr \
    ffmpeg-python \
    git+https://github.com/m-bain/whisperx.git \
    jmespath \
    meilisearch_python_sdk \
    numpy \
    pdf2image \
    pillow \
    pyexiftool \
    pyinotify \
    python-magic \
    rawpy \
    tika \
    wand \
    xxhash

COPY src .
ENTRYPOINT ["python3", "/app/main.py"]
