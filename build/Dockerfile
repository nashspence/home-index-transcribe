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
    openjdk-17-jre-headless \
    poppler-utils \
    tzdata \
    shared-mime-info \
    && apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

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
    wand \
    watchdog \
    xxhash

COPY index.py .
COPY modules .

RUN wget https://repo1.maven.org/maven2/org/apache/tika/tika-app/3.0.0/tika-app-3.0.0.jar -O /tika-app.jar

ENV PYTHONPATH="/app/modules:${PYTHONPATH}"
ENTRYPOINT ["/usr/local/bin/python", "/app/index.py"]
