#!/bin/bash
/usr/local/bin/start-docker.sh
if [ -d /tmp/.ssh ]; then
  cp -r /tmp/.ssh /root/
  chmod 700 /root/.ssh
  chmod 600 /root/.ssh/*
fi
python -m venv venv
/workspace/venv/bin/pip install --upgrade pip
PATH="/workspace/venv/bin:$PATH"
/workspace/venv/bin/pip install -r requirements.txt
source venv/bin/activate