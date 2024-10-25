# Dockerfile

FROM intel/intel-optimized-pytorch:2.4.0-idp-base

RUN apt-get update && apt-get install -y \
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
    tika \
    wand \
    watchdog

COPY index.py .
COPY modules .

RUN wget http://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/2.6.0/tika-server-standard-2.6.0.jar -O /tika-server.jar

ENV TIKA_SERVER_ENDPOINT='http://localhost:9998'
ENV TIKA_CLIENT_ONLY=True
ENV PYTHONPATH="/app/modules:${PYTHONPATH}"
ENTRYPOINT ["/usr/local/bin/python", "/app/index.py"]
