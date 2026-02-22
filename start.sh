#!/bin/bash
# Start the StatsBomb Viz API + frontend from the main repo root.
REPO="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO/venv"

echo "Starting StatsBomb Viz API..."
echo "  Repo:  $REPO"
echo "  Open:  http://127.0.0.1:8000"
echo ""

cd "$REPO"
source "$VENV/bin/activate"
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
