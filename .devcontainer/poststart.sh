#!/bin/bash
/usr/local/bin/start-docker.sh
if [ -d /home/ns/.ssh ]; then
  cp -r /home/ns/.ssh /root/
  chmod 700 /root/.ssh
  chmod 600 /root/.ssh/*
fi