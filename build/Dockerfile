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
    poppler-utils \
    tzdata \
    shared-mime-info \
    && apt-get clean

WORKDIR /app

RUN pip install \
    Pillow \
    aiohttp \
    ffmpeg-python \
    flatdict \
    git+https://github.com/m-bain/whisperx.git \
    jmespath \
    meilisearch_python_sdk \
    numpy \
    pdf2image \
    pyexiftool \
    pymediainfo \
    python-magic \
    tika \
    wand \
    watchdog \
    xxhash

COPY index.py .
COPY modules .

ENV PYTHONPATH="/app/modules:${PYTHONPATH}"

ENTRYPOINT ["/usr/local/bin/python", "/app/index.py"]
