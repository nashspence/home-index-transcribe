# Dockerfile

FROM intel/intel-optimized-pytorch:2.4.0-idp-base

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    shared-mime-info \
    libmagic1 \
    file \
    ffmpeg \
    imagemagick \
    poppler-utils \
    libgomp1 \
    tesseract-ocr \
    git \
    dcraw \
    && apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /app

RUN pip install meilisearch_python_sdk \
    python-magic \
    tika \
    git+https://github.com/m-bain/whisperx.git \
    ffmpeg-python \
    watchdog \
    pdf2image \
    numpy \
    scikit-image \
    rawpy \
    Pillow \
    wand \
    pytesseract

COPY index.py .
COPY modules .

RUN wget http://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/2.6.0/tika-server-standard-2.6.0.jar -O /tika-server.jar

ENV TIKA_SERVER_ENDPOINT='http://localhost:9998'
ENV TIKA_CLIENT_ONLY=True
ENV PYTHONPATH="/app/modules:${PYTHONPATH}"
ENTRYPOINT ["/usr/local/bin/python", "/app/index.py"]
