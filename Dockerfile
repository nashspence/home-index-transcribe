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

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src .
ENTRYPOINT ["python3", "/app/main.py"]
