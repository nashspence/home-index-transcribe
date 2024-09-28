#!/bin/bash

# Default cron schedule set to 3 AM every day
CRON_SCHEDULE="${CRON_SCHEDULE:-0 3 * * *}"

# Write out the crontab entry
echo "$CRON_SCHEDULE /app/cron-index.sh >> /app/data/cron.log 2>&1" > /etc/cron.d/index-cron

# Give execution rights on the cron job
chmod 0644 /etc/cron.d/index-cron

# Apply cron job
crontab /etc/cron.d/index-cron

# Create the log file to be able to run tail
touch /app/data/cron.log

# Start the cron service
cron &

/app/cron-index.sh >> /app/data/cron.log 2>&1 &

# Tail the cron log to keep the container running and output logs
tail -f /app/data/cron.log &

sleep 5

wait
