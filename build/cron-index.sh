#!/bin/bash

# Log a message with a timestamp and log level (INFO, WARN, ERROR)
log() {
    local level=$1
    shift
    echo "$(date +'%Y-%m-%d %H:%M:%S') [$level] $*" >> /app/data/cron.log 2>&1
}

# Initial log entry to indicate the script is running
log "INFO" "Script started."

# Get the Process Group ID (PGID) of the running script (if any)
RUNNING_PGID=$(pgrep -f "/app/index.py" | head -n 1)

if [ -n "$RUNNING_PGID" ]; then
    PGID=$(ps -o pgid= -p "$RUNNING_PGID" | awk '{print $1}')
    log "INFO" "Found running process with PID: $RUNNING_PGID and PGID: $PGID"

    # Ensure PGID is not 1 (init) and not empty
    if [ "$PGID" != "1" ] && [ -n "$PGID" ]; then
        log "INFO" "Attempting to terminate process group with PGID: $PGID"
        
        # Try SIGTERM first to allow for graceful shutdown
        if kill -TERM -"$PGID" >> /app/data/cron.log 2>&1; then
            log "INFO" "Sent SIGTERM to PGID: $PGID"
        else
            log "ERROR" "Failed to send SIGTERM to PGID: $PGID"
        fi
        
        # Wait for a few seconds to see if the process group shuts down
        sleep 5
        
        # If the process group is still running, forcefully kill it with SIGKILL
        if ps -p "$PGID" > /dev/null 2>&1; then
            log "WARN" "Process group $PGID did not terminate. Forcing kill with SIGKILL."
            if kill -9 -"$PGID" >> /app/data/cron.log 2>&1; then
                log "INFO" "Successfully killed PGID: $PGID with SIGKILL."
            else
                log "ERROR" "Failed to kill PGID: $PGID with SIGKILL."
            fi
        else
            log "INFO" "Process group $PGID terminated gracefully."
        fi
    else
        log "ERROR" "Invalid PGID: $PGID. Skipping kill attempt."
    fi
else
    log "INFO" "No running process found for /app/index.py."
fi

# Start a new instance of the script in a new process group
log "INFO" "Starting a new instance of /app/index.py in its own process group."

if setsid /usr/local/bin/python /app/index.py >> /app/data/cron.log 2>&1; then
    log "INFO" "Successfully started a new instance of /app/index.py"
else
    log "ERROR" "Failed to start a new instance of /app/index.py"
fi

log "INFO" "Script execution completed."
