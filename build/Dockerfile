# Dockerfile

FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    libmagic1 \
    ffmpeg \
    && apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /app

RUN pip install meilisearch_python_sdk \
    python-magic \
    tika \
    faster-whisper \
    ffmpeg-python

COPY index.py .

ENTRYPOINT ["/usr/local/bin/python", "/app/index.py"]
