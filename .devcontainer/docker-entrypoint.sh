#!/bin/bash
set -e

# Forward the host Docker socket if it exists
if [ -S /var/run/docker.sock ]; then
    DOCKER_GROUP_ID=$(stat -c '%g' /var/run/docker.sock)
    groupadd -for -g "$DOCKER_GROUP_ID" hostdocker
    usermod -aG hostdocker devcontainer
fi

exec "$@"
