FROM python:slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    attr \
    file \
    git \
    tzdata \
    shared-mime-info \
    && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src .
ENTRYPOINT ["python3", "/app/main.py"]
