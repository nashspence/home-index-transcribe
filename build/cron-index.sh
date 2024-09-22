#!/bin/bash

# Get the Process Group ID (PGID) of the running script (if any)
RUNNING_PGID=$(pgrep -f "/app/index.py" | xargs ps -o pgid= | awk '{print $1}' | uniq)

if [ ! -z "$RUNNING_PGID" ]; then
    echo "Killing process group with PGID: $RUNNING_PGID" >> /var/log/cron.log 2>&1
    
    # Try SIGTERM first to allow for graceful shutdown
    kill -TERM -"$RUNNING_PGID" >> /var/log/cron.log 2>&1
    
    # Wait for a few seconds to see if the process group shuts down
    sleep 5
    
    # If the process group is still running, forcefully kill it with SIGKILL
    if ps -p "$RUNNING_PGID" > /dev/null 2>&1; then
        echo "Process group didn't terminate, forcing kill" >> /var/log/cron.log 2>&1
        kill -9 -"$RUNNING_PGID" >> /var/log/cron.log 2>&1
    fi
else
    echo "No running process group found, starting a new instance." >> /var/log/cron.log 2>&1
fi

# Run the python script in its own process group
setsid /usr/local/bin/python /app/index.py >> /var/log/cron.log 2>&1
