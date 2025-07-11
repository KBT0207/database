#!/bin/bash
cd /home/vivek/test/database
LOG_DIR="/home/vivek/cron"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/cron.log"

{
  echo "=== Run started at $(date) ==="
  /home/vivek/test/database/.venv/bin/python -u main.py
  echo "=== Run finished at $(date) ==="
} 2>&1 | tee -a "$LOG_FILE"
