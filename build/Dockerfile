# Dockerfile

FROM python:3.9-slim

# Install cron, OpenJDK 17, libmagic, FFmpeg, and git
RUN apt-get update && apt-get install -y \
    cron \
    openjdk-17-jre-headless \
    libmagic1 \
    file \
    ffmpeg \
    git \
    && apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /app

RUN pip install meilisearch_python_sdk
RUN pip install python-magic
RUN pip install tika
RUN pip install faster-whisper
RUN pip install ffmpeg-python

# Copy application files
COPY index.py .

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
