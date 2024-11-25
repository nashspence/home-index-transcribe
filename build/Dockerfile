# Dockerfile

FROM intel/intel-optimized-pytorch:2.4.0-idp-base

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
    Pillow \
    ffmpeg-python \
    git+https://github.com/m-bain/whisperx.git \
    jmespath \
    meilisearch_python_sdk \
    numpy \
    pdf2image \
    pyexiftool \
    pyinotify \
    python-magic \
    tika \
    wand \
    xxhash

COPY main.py .
COPY shared.py .
COPY inotify_module.py .
COPY process_tasks_module.py .
COPY modules ./modules/

ENV PYTHONPATH="/app/modules:${PYTHONPATH}"

ENTRYPOINT ["/usr/local/bin/python", "/app/main.py"]
