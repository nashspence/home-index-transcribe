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
    procps \
    psmisc \
    && apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Set the working directory to /app
WORKDIR /app

# Install Python dependencies
RUN pip install meilisearch_python_sdk \
    python-magic \
    tika \
    faster-whisper \
    ffmpeg-python

# Copy application files
COPY index.py .

# Copy the entrypoint script and make it executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy the cron wrapper script (cron-index.sh) and make it executable
COPY cron-index.sh /app/cron-index.sh
RUN chmod +x /app/cron-index.sh

# Set the entrypoint to the entrypoint.sh script
ENTRYPOINT ["/entrypoint.sh"]
